"""
Database Configuration and Connection Module - Multi-Node Support

This module handles all database connections for the Financial Reports Dashboard.
It supports both local MySQL connections and Google Cloud SQL connections for 3 nodes.
Works with both Streamlit secrets and .env files.
Uses st.connection() for better connection management when running in Streamlit.
Includes distributed locking functionality for coordinating transactions across nodes.
"""

import mysql.connector
import pandas as pd
import hashlib
from datetime import datetime
from dotenv import load_dotenv
import os
from typing import Dict, Any, List

# Load environment variables from .env file
load_dotenv()

# Cloud SQL connector will be initialized only when needed
_connector = None
_streamlit_connections = {}  # Cache for st.connection per node

def _is_running_in_streamlit():
    """
    Check if code is running inside a Streamlit app.

    Returns:
        bool: True if running in Streamlit, False otherwise
    """
    try:
        import streamlit as st
        return hasattr(st, 'secrets')
    except ImportError:
        return False


def _get_config_value(key, default=''):
    """
    Get configuration value from Streamlit secrets or environment variables.
    Streamlit secrets take precedence if available.

    Args:
        key (str): Configuration key name
        default: Default value if key not found

    Returns:
        Configuration value
    """
    try:
        # Try to import streamlit and use secrets
        import streamlit as st
        if hasattr(st, 'secrets') and key in st.secrets:
            value = st.secrets[key]
            # Handle boolean values from TOML (they come as Python bool, not string)
            if isinstance(value, bool):
                return value
            return value
    except (ImportError, FileNotFoundError, KeyError):
        pass

    # Fall back to environment variables
    return os.getenv(key, default)


# Query Cache Configuration
_cache_enabled_value = _get_config_value('CACHE_ENABLED', 'True')
if isinstance(_cache_enabled_value, bool):
    CACHE_ENABLED = _cache_enabled_value
else:
    CACHE_ENABLED = str(_cache_enabled_value).lower() == 'true'
CACHE_TTL_SECONDS = int(_get_config_value('CACHE_TTL_SECONDS', '3600'))
_query_cache = {}  # In-memory cache storage per node

# Node Selection Configuration
# For Streamlit deployment: Use NODE_USE from secrets.toml (must be set manually: 1, 2, or 3)
# For local use: Use NODE_USE from environment variable (set by run.py <node_number>)
def get_active_node():
    """
    Get the active node number for this instance.

    Returns:
        int: Node number (1, 2, or 3). Defaults to 1 if not specified.
    """
    node_use = _get_config_value('NODE_USE', '1')
    try:
        node = int(node_use)
        if node not in [1, 2, 3]:
            print(f"Warning: Invalid NODE_USE value '{node}'. Defaulting to node 1.")
            return 1
        return node
    except ValueError:
        print(f"Warning: Invalid NODE_USE value '{node_use}'. Defaulting to node 1.")
        return 1

# Active node - this determines which database node this instance connects to
NODE_USE = get_active_node()

# Database Configuration
# Choose connection method by setting USE_CLOUD_SQL environment variable
_use_cloud_sql_value = _get_config_value('USE_CLOUD_SQL', 'False')
# Handle both boolean (from TOML) and string (from env vars) values
if isinstance(_use_cloud_sql_value, bool):
    USE_CLOUD_SQL = _use_cloud_sql_value
else:
    USE_CLOUD_SQL = str(_use_cloud_sql_value).lower() == 'true'

# Auto-detect: If running on Streamlit Cloud and cloud connections exist, use cloud
if _is_running_in_streamlit():
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and 'connections' in st.secrets:
            # Check if cloud connection exists
            if 'mysql' in st.secrets.connections:
                cloud_host = st.secrets.connections.mysql.get('host', '')
                # If host is not localhost and not empty, we should use cloud
                if cloud_host and cloud_host != 'localhost':
                    USE_CLOUD_SQL = True
                    print(f"[DB_CONFIG] Auto-detected Cloud SQL from connection config (host: {cloud_host})")
    except Exception as e:
        print(f"[DB_CONFIG] Could not auto-detect Cloud SQL: {e}")

# Debug logging for deployment troubleshooting
print(f"[DB_CONFIG] USE_CLOUD_SQL raw value: {_use_cloud_sql_value} (type: {type(_use_cloud_sql_value).__name__})")
print(f"[DB_CONFIG] USE_CLOUD_SQL final value: {USE_CLOUD_SQL}")

# Function to get config from connections or legacy secrets
def _get_node_config_from_connections():
    """
    Get database configuration from st.secrets.connections if available.
    This is the preferred method for Streamlit Cloud deployment.
    
    Returns:
        dict: Dictionary mapping node numbers to their cloud configs, or None if not available
    """
    if not _is_running_in_streamlit():
        return None
    
    try:
        import streamlit as st
        if not hasattr(st, 'secrets') or 'connections' not in st.secrets:
            return None
        
        connections = st.secrets.connections
        configs = {}
        
        # Node 1 - use 'mysql' connection
        if 'mysql' in connections:
            conn = connections.mysql
            configs[1] = {
                "host": conn.get('host'),
                "port": int(conn.get('port')),
                "user": conn.get('username'),
                "password": conn.get('password'),
                "database": conn.get('database')
            }
            print(f"[DB_CONFIG] Node 1 config from st.secrets.connections.mysql: {configs[1]['host']}")
        
        # Node 2 - use 'mysql_node2' connection
        if 'mysql_node2' in connections:
            conn = connections.mysql_node2
            configs[2] = {
                "host": conn.get('host'),
                "port": int(conn.get('port')),
                "user": conn.get('username'),
                "password": conn.get('password'),
                "database": conn.get('database')
            }
            print(f"[DB_CONFIG] Node 2 config from st.secrets.connections.mysql_node2: {configs[2]['host']}")
        
        # Node 3 - use 'mysql_node3' connection
        if 'mysql_node3' in connections:
            conn = connections.mysql_node3
            configs[3] = {
                "host": conn.get('host'),
                "port": int(conn.get('port')),
                "user": conn.get('username'),
                "password": conn.get('password'),
                "database": conn.get('database')
            }
            print(f"[DB_CONFIG] Node 3 config from st.secrets.connections.mysql_node3: {configs[3]['host']}")
        
        return configs if configs else None
    except Exception as e:
        print(f"[DB_CONFIG] Error reading from st.secrets.connections: {e}")
        return None

# Try to get configs from st.secrets.connections first (preferred for Streamlit Cloud)
_node_configs_from_connections = _get_node_config_from_connections()

if _node_configs_from_connections:
    # Use configs from st.secrets.connections
    print("[DB_CONFIG] Using configuration from st.secrets.connections")
    # # Force USE_CLOUD_SQL to True when using connections
    # USE_CLOUD_SQL = True
    print("[DB_CONFIG] Forcing USE_CLOUD_SQL = True (using st.secrets.connections)")
    NODE_CONFIGS = {node: {"cloud": config} for node, config in _node_configs_from_connections.items()}
else:
    # Fall back to legacy configuration method
    print("[DB_CONFIG] Using legacy configuration from flat secrets/env vars")
    
    # Read and log config values for debugging
    _cloud_host = _get_config_value('CLOUD_DB_HOST', '')
    _cloud_user = _get_config_value('CLOUD_DB_USER', '')
    _local_host = _get_config_value('LOCAL_DB_HOST', 'localhost')
    _local_user = _get_config_value('LOCAL_DB_USER', 'user')
    print(f"[DB_CONFIG] CLOUD_DB_HOST: '{_cloud_host}'")
    print(f"[DB_CONFIG] CLOUD_DB_USER: '{_cloud_user}'")
    print(f"[DB_CONFIG] LOCAL_DB_HOST: '{_local_host}'")
    print(f"[DB_CONFIG] LOCAL_DB_USER: '{_local_user}'")

    # Node 1 Configuration
    CLOUD_SQL_CONFIG_NODE1 = {
        "host": _get_config_value('CLOUD_DB_HOST'),
        "port": int(_get_config_value('CLOUD_DB_PORT')),
        "user": _get_config_value('CLOUD_DB_USER'),
        "password": _get_config_value('CLOUD_DB_PASSWORD'),
        "database": _get_config_value('CLOUD_DB_NAME')
    }

    # Node 2 Configuration
    CLOUD_SQL_CONFIG_NODE2 = {
        "host": _get_config_value('CLOUD_DB_HOST_NODE2'),
        "port": int(_get_config_value('CLOUD_DB_PORT_NODE2')),
        "user": _get_config_value('CLOUD_DB_USER_NODE2'),
        "password": _get_config_value('CLOUD_DB_PASSWORD_NODE2'),
        "database": _get_config_value('CLOUD_DB_NAME_NODE2')
    }

    # Node 3 Configuration
    CLOUD_SQL_CONFIG_NODE3 = {
        "host": _get_config_value('CLOUD_DB_HOST_NODE3'),
        "port": int(_get_config_value('CLOUD_DB_PORT_NODE3')),
        "user": _get_config_value('CLOUD_DB_USER_NODE3'),
        "password": _get_config_value('CLOUD_DB_PASSWORD_NODE3'),
        "database": _get_config_value('CLOUD_DB_NAME_NODE3')
    }

    NODE_CONFIGS = {
        1: {"cloud": CLOUD_SQL_CONFIG_NODE1},
        2: {"cloud": CLOUD_SQL_CONFIG_NODE2},
        3: {"cloud": CLOUD_SQL_CONFIG_NODE3}
    }

# Debug logging for configuration
print(f"[DB_CONFIG] Active Node: {NODE_USE}")
for node_num in [1, 2, 3]:
    if node_num in NODE_CONFIGS:
        # Always use cloud config since we only have cloud configs now
        if "cloud" in NODE_CONFIGS[node_num]:
            active_config = NODE_CONFIGS[node_num]["cloud"]
            print(f"[DB_CONFIG] Node {node_num} (cloud): {active_config['host']}:{active_config['port']}/{active_config['database']}")
        else:
            print(f"[DB_CONFIG] Node {node_num}: Configuration not found")

# Map node numbers to Streamlit connection names
STREAMLIT_CONN_NAMES = {
    1: {"cloud": "mysql", "local": "mysql_local"},
    2: {"cloud": "mysql_node2", "local": "mysql_local_node2"},
    3: {"cloud": "mysql_node3", "local": "mysql_local_node3"}
}


def _generate_cache_key(query, node):
    """
    Generate a unique cache key for a query and node combination.

    Args:
        query (str): SQL query string
        node (int): Node number (1, 2, or 3)

    Returns:
        str: MD5 hash of the query and node
    """
    # Normalize query: strip whitespace and convert to lowercase
    normalized_query = ' '.join(query.strip().lower().split())
    # Include node number in cache key
    cache_input = f"node{node}:{normalized_query}"
    return hashlib.md5(cache_input.encode()).hexdigest()


def _is_cache_valid(cache_entry):
    """
    Check if a cache entry is still valid based on TTL.

    Args:
        cache_entry (dict): Cache entry with 'timestamp' and 'data' keys

    Returns:
        bool: True if cache is valid, False if expired
    """
    if not CACHE_ENABLED:
        return False

    timestamp = cache_entry.get('timestamp')
    if not timestamp:
        return False

    age = (datetime.now() - timestamp).total_seconds()
    return age < CACHE_TTL_SECONDS


def get_node_config(node):
    """
    Get the configuration for a specific node.

    Args:
        node (int): Node number (1, 2, or 3)

    Returns:
        dict: Configuration dictionary for the specified node

    Raises:
        ValueError: If node number is invalid
    """
    if node not in NODE_CONFIGS:
        raise ValueError(f"Invalid node number: {node}. Must be 1, 2, or 3.")

    # Always use cloud config since we only have cloud configs now
    if "cloud" in NODE_CONFIGS[node]:
        return NODE_CONFIGS[node]["cloud"]
    else:
        # Fallback for legacy code (shouldn't reach here in production)
        config_type = "cloud" if USE_CLOUD_SQL else "local"
        return NODE_CONFIGS[node][config_type]


def get_db_connection(node):
    """
    Establish and return a database connection for a specific node.

    Args:
        node (int): Node number (1, 2, or 3)

    Returns:
        mysql.connector.connection: Database connection object

    Raises:
        Exception: If connection fails
    """
    config = get_node_config(node)
    config_type = "Cloud SQL" if USE_CLOUD_SQL else "Local"

    # Debug logging
    print(f"[DB_CONFIG] Connecting to {config_type} Node {node}: {config['host']}:{config['port']}/{config['database']}")

    try:
        conn = mysql.connector.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config["password"],
            database=config["database"],
            autocommit=False,
            connect_timeout=10  # Add timeout to prevent hanging
        )
        print(f"[DB_CONFIG] Successfully connected to {config_type} Node {node}")
        return conn
    except mysql.connector.Error as db_err:
        error_code = db_err.errno if hasattr(db_err, 'errno') else 'Unknown'
        error_msg = (
            f"Failed to connect to {config_type} database (Node {node})\n"
            f"Host: {config['host']}:{config['port']}\n"
            f"Database: {config['database']}\n"
            f"User: {config['user']}\n"
            f"Error Code: {error_code}\n"
            f"Error: {str(db_err)}\n\n"
            f"Common solutions:\n"
            f"1. Check if the database server is running\n"
            f"2. Verify host/port are correct\n"
            f"3. Ensure user has proper permissions\n"
            f"4. Check firewall/network settings\n"
            f"5. For Cloud SQL: verify IP whitelist and public IP access"
        )
        raise Exception(error_msg)
    except Exception as e:
        raise Exception(f"Failed to connect to {config_type} database (Node {node}) at {config['host']}:{config['port']}\n"
                      f"Error: {str(e)}")


def fetch_data(query, node, ttl=9999):
    """
    Execute a SQL query and return results as a pandas DataFrame from a specific node.
    Uses st.connection() when running in Streamlit for better caching and connection management.
    Falls back to direct MySQL connection when not in Streamlit.

    Args:
        query (str): SQL query to execute
        node (int): Node number (1, 2, or 3) to query from
        ttl (int): Time-to-live for cached results in seconds (default: 9999)

    Returns:
        pandas.DataFrame: Query results

    Raises:
        Exception: If query execution fails
    """
    # Validate node number
    if node not in NODE_CONFIGS:
        raise ValueError(f"Invalid node number: {node}. Must be 1, 2, or 3.")

    # Try to use Streamlit connection if available
    if _is_running_in_streamlit():
        conn_name = None
        try:
            import streamlit as st
            # Get the appropriate connection name for this node
            config_type = "cloud" if USE_CLOUD_SQL else "local"
            conn_name = STREAMLIT_CONN_NAMES[node][config_type]

            # Check if the connection exists in secrets before trying to use it
            if not hasattr(st, 'secrets') or 'connections' not in st.secrets or conn_name not in st.secrets.connections:
                # Connection not configured in secrets.toml, fall back to manual connection
                print(f"[DB_CONFIG] Warning: [connections.{conn_name}] not found in secrets.toml, using manual connection")
                # Skip to manual connection below
                pass
            else:
                # Use st.connection for automatic caching and connection management
                conn = st.connection(conn_name, type='sql')
                # Execute query with built-in caching (ttl in seconds)
                return conn.query(query, ttl=ttl)
        except Exception as e:
            # Log the error but fall back to manual connection instead of failing
            config = get_node_config(node)
            config_type_name = "Cloud SQL" if USE_CLOUD_SQL else "Local"
            conn_name_msg = f"[connections.{conn_name}]" if conn_name else "connection"
            print(f"[DB_CONFIG] Streamlit connection failed, falling back to manual connection: {str(e)}")
            # Fall through to manual connection below

    # Not in Streamlit - use manual connection with custom caching
    cache_key = _generate_cache_key(query, node)

    # Check if valid cached result
    if CACHE_ENABLED and cache_key in _query_cache:
        cache_entry = _query_cache[cache_key]
        if _is_cache_valid(cache_entry):
            return cache_entry['data'].copy()
        else:
            del _query_cache[cache_key]

    # Cache miss or expired - fetch from database
    conn = None
    cursor = None

    try:
        conn = get_db_connection(node)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        data = cursor.fetchall()
        result_df = pd.DataFrame(data)

        # Store in cache
        if CACHE_ENABLED:
            _query_cache[cache_key] = {
                'timestamp': datetime.now(),
                'data': result_df.copy(),
                'query': query[:100],
                'node': node
            }

        return result_df

    except mysql.connector.Error as db_err:
        config = get_node_config(node)
        config_type = "Cloud SQL" if USE_CLOUD_SQL else "Local"
        error_msg = (
            f"Database error while fetching data from {config_type} (Node {node}) "
            f"({config['host']}:{config['port']}/{config['database']}): {str(db_err)}\n"
            f"Query: {query[:200]}..."
        )
        raise Exception(error_msg)

    except Exception as e:
        config = get_node_config(node)
        config_type = "Cloud SQL" if USE_CLOUD_SQL else "Local"
        error_msg = (
            f"Failed to fetch data from {config_type} (Node {node}) "
            f"({config['host']}:{config['port']}/{config['database']}): {str(e)}\n"
            f"Error type: {type(e).__name__}"
        )
        raise Exception(error_msg)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def execute_query(query, node, isolation_level="READ COMMITTED"):
    """
    Execute a write query (INSERT, UPDATE, DELETE) on specified database node
    
    Args:
        query: SQL query string (INSERT, UPDATE, DELETE)
        node: Node number (1, 2, or 3)
        isolation_level: Transaction isolation level
    
    Returns:
        Number of affected rows or True on success
    """
    # Validate node number
    if node not in NODE_CONFIGS:
        raise ValueError(f"Invalid node number: {node}. Must be 1, 2, or 3.")
    
    # Get config from centralized node config
    config = get_node_config(node)

    conn = None
    cursor = None
    
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # Set isolation level
        cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
        
        # Start transaction
        cursor.execute("START TRANSACTION")
        
        # Execute the query
        cursor.execute(query)
        
        # Commit transaction
        conn.commit()
        
        # Return affected rows count
        affected_rows = cursor.rowcount
        
        return affected_rows
        
    except mysql.connector.Error as e:
        if conn:
            conn.rollback()
        raise Exception(f"MySQL Error for Node {node}: {str(e)}")
    
    except Exception as e:
        if conn:
            conn.rollback()
        raise Exception(f"Error executing query on Node {node}: {str(e)}")
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def test_connection(node):
    """
    Test the database connection for a specific node.

    Args:
        node (int): Node number (1, 2, or 3) to test

    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        conn = get_db_connection(node)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        print(f"Node {node} database connection successful!")
        return True
    except Exception as e:
        print(f"Node {node} database connection failed: {str(e)}")
        return False


# ============================================================================
# DISTRIBUTED LOCKING FUNCTIONS
# ============================================================================

# Initialize the distributed lock manager with node configurations
_lock_manager = None

def create_dedicated_connection(node: int, isolation_level: str = "REPEATABLE READ") -> mysql.connector.connection.MySQLConnection:
    """
    Create a dedicated connection with specific isolation level.
    Use this for concurrent transaction testing.

    Args:
        node: Node number (1, 2, or 3)
        isolation_level: Transaction isolation level

    Returns:
        MySQL connection with isolation level set
    """
    conn = get_db_connection(node)
    cursor = conn.cursor()
    cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
    cursor.close()
    return conn

def check_connectivity() -> Dict[int, bool]:
    """
    Check connectivity to all nodes.

    Returns:
        dict mapping node numbers to connectivity status
    """
    status = {}

    for node in [1, 2, 3]:
        try:
            conn = get_db_connection(node)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            status[node] = True
        except Exception as e:
            print(f"Node {node} connectivity check failed: {e}")
            status[node] = False

    return status


def get_max_trans_id_multi_node() -> Dict[str, Any]:
    """
    Query all available nodes for MAX(trans_id) and return the highest value.

    Multi-master replication rules:
    - If node 1 is down but nodes 2 and 3 are up: continue with queries
    - If node 1 is up but nodes 2 and 3 are both down: continue with queries
    - If node 1 and node 2 are down, OR node 1 and node 3 are down: abort (insufficient nodes)

    Returns:
        dict: {
            'status': 'success' | 'failed',
            'max_trans_id': int (highest trans_id found),
            'available_nodes': list of available node numbers,
            'node_values': dict mapping node numbers to their max trans_id values,
            'error': str (only if status is 'failed')
        }
    """
    # Check connectivity to all nodes
    connectivity = check_connectivity()
    available_nodes = [node for node, is_up in connectivity.items() if is_up]

    # Apply multi-master rules
    node1_up = connectivity.get(1, False)
    node2_up = connectivity.get(2, False)
    node3_up = connectivity.get(3, False)

    # Check if we should abort based on failure rules
    if (not node1_up and not node2_up) or (not node1_up and not node3_up):
        return {
            'status': 'failed',
            'max_trans_id': 0,
            'available_nodes': available_nodes,
            'node_values': {},
            'error': 'Insufficient nodes available. Node 1 and at least one other node must be up.'
        }

    # If no nodes are available at all
    if not available_nodes:
        return {
            'status': 'failed',
            'max_trans_id': 0,
            'available_nodes': [],
            'node_values': {},
            'error': 'All database nodes are down.'
        }

    # Query each available node for MAX(trans_id)
    node_values = {}
    max_trans_id = 0

    for node in available_nodes:
        try:
            conn = get_db_connection(node)
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT COALESCE(MAX(trans_id), 0) as max_id FROM trans FOR UPDATE")
            result = cursor.fetchone()
            cursor.close()
            conn.close()

            if result:
                node_max_id = int(result['max_id'])
                node_values[node] = node_max_id
                max_trans_id = max(max_trans_id, node_max_id)
        except Exception as e:
            print(f"Error querying Node {node} for max trans_id: {e}")
            # Node became unavailable during query, continue with others
            node_values[node] = None

    return {
        'status': 'success',
        'max_trans_id': max_trans_id,
        'available_nodes': available_nodes,
        'node_values': node_values,
        'error': None
    }