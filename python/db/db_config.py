"""
Database Configuration and Connection Module - Multi-Node Support

This module handles all database connections for the Financial Reports Dashboard.
It supports both local MySQL connections and Google Cloud SQL connections for 3 nodes.
Works with both Streamlit secrets and .env files.
Uses st.connection() for better connection management when running in Streamlit.
"""

import mysql.connector
import pandas as pd
import hashlib
from datetime import datetime
from dotenv import load_dotenv
import os
from typing import Dict, Any, Optional

# Load environment variables from .env file (fallback for non-Streamlit execution)
load_dotenv()

# Cloud SQL connector will be initialized only when needed
_connector = None
_streamlit_connections = {}  # Cache for st.connection per node


def _is_running_in_streamlit() -> bool:
    """
    Check if code is running inside a Streamlit app.

    Returns:
        bool: True if running in Streamlit, False otherwise
    """
    try:
        import streamlit as st
        # Check if we're actually in a Streamlit runtime (not just imported)
        return hasattr(st, 'secrets')
    except ImportError:
        return False


def _get_config_value(key: str, default: Optional[Any] = None) -> Any:
    """
    Get configuration value from Streamlit secrets or environment variables.
    Streamlit secrets take precedence if available.

    According to Streamlit docs:
    - Root-level secrets are accessible via st.secrets[key] or st.secrets.key
    - Root-level secrets are also available as environment variables
    - Section secrets are accessible via st.secrets.section.key

    Args:
        key: Configuration key name
        default: Default value if key not found (raises error if None and not found)

    Returns:
        Configuration value

    Raises:
        KeyError: If key not found and no default provided
    """
    value = None
    found = False

    # Try Streamlit secrets first
    if _is_running_in_streamlit():
        try:
            import streamlit as st
            # Try to get from st.secrets using attribute access
            if hasattr(st.secrets, key):
                value = getattr(st.secrets, key)
                found = True
            # Also try dictionary access for backward compatibility
            elif key in st.secrets:
                value = st.secrets[key]
                found = True
        except (ImportError, FileNotFoundError, KeyError, AttributeError):
            pass

    # Fall back to environment variables
    if not found:
        env_value = os.getenv(key)
        if env_value is not None:
            value = env_value
            found = True

    # Return value or default
    if found:
        return value
    elif default is not None:
        return default
    else:
        raise KeyError(f"Configuration key '{key}' not found in secrets or environment variables")


# ============================================================================
# CONFIGURATION LOADING
# ============================================================================

def _parse_bool(value: Any) -> bool:
    """Parse boolean from various input types."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'on')
    return bool(value)


# Cache Configuration
CACHE_ENABLED = _parse_bool(_get_config_value('CACHE_ENABLED', True))
CACHE_TTL_SECONDS = int(_get_config_value('CACHE_TTL_SECONDS', 9999))
_query_cache = {}  # In-memory cache storage per node

# Node Selection (which node this instance connects to)
NODE_USE = int(_get_config_value('NODE_USE', 1))
if NODE_USE not in [1, 2, 3]:
    print(f"[DB_CONFIG] Warning: Invalid NODE_USE value '{NODE_USE}'. Defaulting to 1.")
    NODE_USE = 1

# Database Configuration Mode (Cloud SQL vs Local Docker)
USE_CLOUD_SQL = _parse_bool(_get_config_value('USE_CLOUD_SQL', False))

print(f"[DB_CONFIG] USE_CLOUD_SQL: {USE_CLOUD_SQL}")
print(f"[DB_CONFIG] Active Node: {NODE_USE}")
print(f"[DB_CONFIG] Cache: {CACHE_ENABLED} (TTL: {CACHE_TTL_SECONDS}s)")


def _get_node_config_from_sections() -> Dict[int, Dict[str, Dict[str, Any]]]:
    """
    Get database configuration from st.secrets sections (preferred method).
    Uses compact section notation: st.secrets.cloud_node1, st.secrets.local_node1, etc.

    Returns:
        dict: Dictionary mapping node numbers to their configs
              Format: {1: {"cloud": {...}, "local": {...}}, ...}
    """
    configs = {}

    if _is_running_in_streamlit():
        try:
            import streamlit as st

            # Load configs for all 3 nodes
            for node_num in [1, 2, 3]:
                node_configs = {}

                # Cloud config
                cloud_section = f"cloud_node{node_num}"
                if hasattr(st.secrets, cloud_section):
                    cloud_config = getattr(st.secrets, cloud_section)
                    node_configs["cloud"] = dict(cloud_config)
                    print(f"[DB_CONFIG] Loaded cloud config for Node {node_num} from st.secrets.{cloud_section}")

                # Local config
                local_section = f"local_node{node_num}"
                if hasattr(st.secrets, local_section):
                    local_config = getattr(st.secrets, local_section)
                    node_configs["local"] = dict(local_config)
                    print(f"[DB_CONFIG] Loaded local config for Node {node_num} from st.secrets.{local_section}")

                if node_configs:
                    configs[node_num] = node_configs

        except Exception as e:
            print(f"[DB_CONFIG] Error reading from st.secrets sections: {e}")

    return configs if configs else {}


def _get_node_config_from_env() -> Dict[int, Dict[str, Dict[str, Any]]]:
    """
    Get database configuration from environment variables (fallback method).

    Returns:
        dict: Dictionary mapping node numbers to their configs
    """
    configs = {}

    try:
        # Node 1 - Local
        configs[1] = {
            "local": {
                "host": os.getenv('LOCAL_DB_HOST', 'localhost'),
                "port": int(os.getenv('LOCAL_DB_PORT', '3306')),
                "user": os.getenv('LOCAL_DB_USER', 'user'),
                "password": os.getenv('LOCAL_DB_PASSWORD', 'rootpass'),
                "database": os.getenv('LOCAL_DB_NAME', 'node1_db')
            }
        }

        # Node 2 - Local
        configs[2] = {
            "local": {
                "host": os.getenv('LOCAL_DB_HOST_NODE2', 'localhost'),
                "port": int(os.getenv('LOCAL_DB_PORT_NODE2', '3307')),
                "user": os.getenv('LOCAL_DB_USER_NODE2', 'user'),
                "password": os.getenv('LOCAL_DB_PASSWORD_NODE2', 'rootpass'),
                "database": os.getenv('LOCAL_DB_NAME_NODE2', 'node2_db')
            }
        }

        # Node 3 - Local
        configs[3] = {
            "local": {
                "host": os.getenv('LOCAL_DB_HOST_NODE3', 'localhost'),
                "port": int(os.getenv('LOCAL_DB_PORT_NODE3', '3308')),
                "user": os.getenv('LOCAL_DB_USER_NODE3', 'user'),
                "password": os.getenv('LOCAL_DB_PASSWORD_NODE3', 'rootpass'),
                "database": os.getenv('LOCAL_DB_NAME_NODE3', 'node3_db')
            }
        }

        print(f"[DB_CONFIG] Loaded configs from environment variables")

    except Exception as e:
        print(f"[DB_CONFIG] Error reading from environment variables: {e}")

    return configs


# Load node configurations
NODE_CONFIGS = _get_node_config_from_sections()
if not NODE_CONFIGS:
    print("[DB_CONFIG] No Streamlit secrets found, falling back to environment variables")
    NODE_CONFIGS = _get_node_config_from_env()

# Debug logging
for node_num in [1, 2, 3]:
    if node_num in NODE_CONFIGS:
        config_type = "cloud" if USE_CLOUD_SQL else "local"
        if config_type in NODE_CONFIGS[node_num]:
            cfg = NODE_CONFIGS[node_num][config_type]
            print(f"[DB_CONFIG] Node {node_num} ({config_type}): {cfg['host']}:{cfg['port']}/{cfg['database']}")
        else:
            print(f"[DB_CONFIG] Node {node_num}: {config_type} config not found")

# Map node numbers to Streamlit connection names
STREAMLIT_CONN_NAMES = {
    1: {"cloud": "mysql_cloud", "local": "mysql_local"},
    2: {"cloud": "mysql_cloud_node2", "local": "mysql_local_node2"},
    3: {"cloud": "mysql_cloud_node3", "local": "mysql_local_node3"}
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _generate_cache_key(query: str, node: int) -> str:
    """Generate a unique cache key for a query and node combination."""
    normalized_query = ' '.join(query.strip().lower().split())
    cache_input = f"node{node}:{normalized_query}"
    return hashlib.md5(cache_input.encode()).hexdigest()


def _is_cache_valid(cache_entry: Dict[str, Any]) -> bool:
    """Check if a cache entry is still valid based on TTL."""
    if not CACHE_ENABLED:
        return False
    timestamp = cache_entry.get('timestamp')
    if not timestamp:
        return False
    age = (datetime.now() - timestamp).total_seconds()
    return age < CACHE_TTL_SECONDS


def get_node_config(node: int) -> Dict[str, Any]:
    """
    Get the configuration for a specific node.

    Args:
        node: Node number (1, 2, or 3)

    Returns:
        Configuration dictionary for the specified node

    Raises:
        ValueError: If node number is invalid or config not found
    """
    if node not in NODE_CONFIGS:
        raise ValueError(f"Invalid node number: {node}. Must be 1, 2, or 3.")

    config_type = "cloud" if USE_CLOUD_SQL else "local"

    if config_type not in NODE_CONFIGS[node]:
        raise ValueError(
            f"Configuration for Node {node} ({config_type}) not found. "
            f"Please check your secrets.toml or .env file."
        )

    return NODE_CONFIGS[node][config_type]


# ============================================================================
# DATABASE CONNECTION FUNCTIONS
# ============================================================================

def get_db_connection(node: int) -> mysql.connector.connection.MySQLConnection:
    """
    Establish and return a database connection for a specific node.

    Args:
        node: Node number (1, 2, or 3)

    Returns:
        Database connection object

    Raises:
        Exception: If connection fails
    """
    config = get_node_config(node)
    config_type = "Cloud SQL" if USE_CLOUD_SQL else "Local Docker"

    print(f"[DB_CONFIG] Connecting to {config_type} Node {node}: {config['host']}:{config['port']}/{config['database']}")

    try:
        conn = mysql.connector.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config["password"],
            database=config["database"],
            autocommit=False,
            connect_timeout=10,  # Reduced from 10s to 5s for faster failure detection
            pool_name=f"node{node}_pool",  # Enable connection pooling
            pool_size=5,  # Pool of 5 connections per node
            pool_reset_session=True
        )
        print(f"[DB_CONFIG] Successfully connected to {config_type} Node {node}")
        return conn
    except mysql.connector.Error as db_err:
        error_code = db_err.errno if hasattr(db_err, 'errno') else 'Unknown'
        raise Exception(
            f"Failed to connect to {config_type} database (Node {node})\n"
            f"Host: {config['host']}:{config['port']}\n"
            f"Database: {config['database']}\n"
            f"User: {config['user']}\n"
            f"Error Code: {error_code}\n"
            f"Error: {str(db_err)}"
        )
    except Exception as e:
        raise Exception(
            f"Failed to connect to {config_type} database (Node {node})\n"
            f"Location: {config['host']}:{config['port']}\n"
            f"Error: {str(e)}"
        )


def fetch_data(query: str, node: int, ttl: int = 9999) -> pd.DataFrame:
    """
    Execute a SQL query and return results as a pandas DataFrame from a specific node.
    Uses st.connection() when running in Streamlit for better caching.
    Falls back to direct MySQL connection when not in Streamlit.

    Args:
        query: SQL query to execute
        node: Node number (1, 2, or 3) to query from
        ttl: Time-to-live for cached results in seconds

    Returns:
        Query results as DataFrame

    Raises:
        Exception: If query execution fails
    """
    if node not in NODE_CONFIGS:
        raise ValueError(f"Invalid node number: {node}. Must be 1, 2, or 3.")

    # Try to use Streamlit connection if available
    if _is_running_in_streamlit():
        try:
            import streamlit as st
            config_type = "cloud" if USE_CLOUD_SQL else "local"
            conn_name = STREAMLIT_CONN_NAMES[node][config_type]

            # Check if the connection exists in secrets
            if hasattr(st.secrets, 'connections') and hasattr(st.secrets.connections, conn_name):
                conn = st.connection(conn_name, type='sql')
                return conn.query(query, ttl=ttl)
            else:
                print(f"[DB_CONFIG] Connection {conn_name} not found, using manual connection")
        except Exception as e:
            print(f"[DB_CONFIG] Streamlit connection failed: {str(e)}, using manual connection")

    # Manual connection with custom caching
    cache_key = _generate_cache_key(query, node)

    # Check cache
    if CACHE_ENABLED and cache_key in _query_cache:
        cache_entry = _query_cache[cache_key]
        if _is_cache_valid(cache_entry):
            return cache_entry['data'].copy()
        else:
            del _query_cache[cache_key]

    # Fetch from database
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

    except Exception as e:
        config = get_node_config(node)
        config_type = "Cloud SQL" if USE_CLOUD_SQL else "Local Docker"
        raise Exception(
            f"Failed to fetch data from {config_type} (Node {node}): {str(e)}\n"
            f"Query: {query[:200]}..."
        )

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def execute_query(query: str, node: int, isolation_level: str = "READ COMMITTED") -> int:
    """
    Execute a write query (INSERT, UPDATE, DELETE) on specified database node.

    Args:
        query: SQL query string (INSERT, UPDATE, DELETE)
        node: Node number (1, 2, or 3)
        isolation_level: Transaction isolation level
    
    Returns:
        Number of affected rows
    """
    if node not in NODE_CONFIGS:
        raise ValueError(f"Invalid node number: {node}. Must be 1, 2, or 3.")
    
    config = get_node_config(node)
    conn = None
    cursor = None
    
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
        cursor.execute("START TRANSACTION")
        cursor.execute(query)
        conn.commit()
        return cursor.rowcount

    except Exception as e:
        if conn:
            conn.rollback()
        raise Exception(f"Error executing query on Node {node}: {str(e)}")
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def test_connection(node: int) -> bool:
    """
    Test the database connection for a specific node.

    Args:
        node: Node number (1, 2, or 3) to test

    Returns:
        True if connection successful, False otherwise
    """
    try:
        conn = get_db_connection(node)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
        print(f"[DB_CONFIG] Node {node} connection test: SUCCESS")
        return True
    except Exception as e:
        print(f"[DB_CONFIG] Node {node} connection test: FAILED - {str(e)}")
        return False



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


# ============================================================================
# MULTI-NODE UTILITIES
# ============================================================================

def check_connectivity() -> Dict[int, bool]:
    """
    Check connectivity to all nodes.

    Returns:
        Dictionary mapping node numbers to connectivity status
    """
    status = {}
    for node in [1, 2, 3]:
        status[node] = test_connection(node)
    return status


def get_max_trans_id_multi_node() -> Dict[str, Any]:
    """
    Query all available nodes for MAX(trans_id) and return the highest value.

    Multi-master replication rules:
    - If node 1 is down but nodes 2 and 3 are up: continue
    - If node 1 is up but nodes 2 and 3 are both down: continue
    - If node 1 and node 2 are down, OR node 1 and node 3 are down: abort

    Returns:
        Dictionary with status, max_trans_id, available_nodes, node_values, and error
    """
    connectivity = check_connectivity()
    available_nodes = [node for node, is_up in connectivity.items() if is_up]

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
            print(f"[DB_CONFIG] Error querying Node {node} for max trans_id: {e}")
            node_values[node] = None

    return {
        'status': 'success',
        'max_trans_id': max_trans_id,
        'available_nodes': available_nodes,
        'node_values': node_values,
        'error': None
    }

if __name__ == "__main__":
    # Test connections to all nodes
    for node in [1, 2, 3]:
        test_connection(node)