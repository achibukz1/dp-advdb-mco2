"""
View Transactions Page - Read Operation
Supports viewing transactions even when one node is offline by combining data from available nodes
"""
import streamlit as st
import pandas as pd
import time
from datetime import datetime
import sys
import os

# Add parent directory to path for imports (fixes Streamlit Cloud deployment)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from python.utils.server_ping import NodePinger
from python.db.db_config import fetch_data


def render(get_node_for_account, log_transaction):
    """Render the View Transactions page"""
    st.title("View Transactions (Read Operation)")

    st.markdown("""
    Browse transaction records from the database. The system intelligently adapts to node availability:
    
    **Resilient Query Strategy:**
    - 🎯 **Specific Account**: Queries target partition node, falls back to Node 1 if offline
    - 📊 **All Data (Node 1 online)**: Uses complete central database
    - 🔄 **All Data (Node 1 offline)**: Combines Node 2 + Node 3 for complete view
    - ⚡ **Always works** as long as at least one node is online
    """)

    # Configuration
    limit = st.number_input("Number of rows", min_value=10, max_value=1000, value=50)

    # Filter options
    st.subheader("Filter Options")

    col1, col2, col3 = st.columns(3)

    with col1:
        account_id = st.text_input("Account ID", placeholder="Leave empty for all")

    with col2:
        trans_type = st.selectbox("Transaction Type",
                                  ["All", "Credit", "Debit"])

    with col3:
        date_range = st.date_input("Date Range", value=None)

    # Check node status
    pinger = NodePinger()
    node_status = pinger.ping_all_nodes()
    
    # Display node status
    st.subheader("Node Status")
    status_cols = st.columns(3)
    for i, node in enumerate([1, 2, 3]):
        with status_cols[i]:
            if node_status.get(node, False):
                st.success(f"Node {node} Online")
            else:
                st.error(f"Node {node} Offline")

    # Build query based on filters
    base_query = "SELECT * FROM trans WHERE 1=1"
    
    if account_id:
        base_query += f" AND account_id = {account_id}"
        
    if trans_type != "All":
        base_query += f" AND type = '{trans_type}'"

    # Don't add LIMIT to individual queries - we'll limit the combined result
    query_without_limit = base_query
    base_query += f" LIMIT {limit}"

    # Execute button with custom styling
    st.markdown("""
    <style>
    div.stButton > button {
        background-color: #4B5C4B;
        color: white;
        border-color: #4B5C4B;
    }
    div.stButton > button:hover {
        background-color: #3A4A3A;
        border-color: #3A4A3A;
    }
    /* Rollback button styling */
    button[data-testid="baseButton-secondary"]:has(p:contains("Rollback")) {
        background-color: #692727 !important;
        border-color: #692727 !important;
    }
    button[data-testid="baseButton-secondary"]:has(p:contains("Rollback")):hover {
        background-color: #531F1F !important;
        border-color: #531F1F !important;
    }
    </style>
    """, unsafe_allow_html=True)

    btn_col1, btn_col2, btn_col3 = st.columns(3)
    with btn_col1:
        fetch_button = st.button("🔍 Fetch Data", type="primary", use_container_width=True)
    with btn_col2:
        commit_button = st.button("✅ Commit Transaction", type="secondary", use_container_width=True, key="commit_read")
    with btn_col3:
        rollback_button = st.button("↩️ Rollback", type="secondary", use_container_width=True, key="rollback_read")

    if commit_button:
        st.info("ℹ️ View operations are read-only and don't require explicit commits")
        st.success("✅ All data has been successfully retrieved and displayed")

    if rollback_button:
        st.info("ℹ️ View operations are read-only - no rollback needed")
        st.success("🔄 Ready to perform new queries")

    if fetch_button:
        start_time = time.time()
        
        # Determine strategy based on node availability
        online_nodes = [node for node, status in node_status.items() if status]
        offline_nodes = [node for node, status in node_status.items() if not status]
        
        if not online_nodes:
            st.error("❌ All nodes are offline! Cannot retrieve data.")
            return
            
        st.info(f"📡 Online nodes: {online_nodes}, Offline nodes: {offline_nodes}")
        
        try:
            combined_data = pd.DataFrame()
            query_sources = []
            
            with st.spinner("Fetching data from available nodes..."):
                
                if account_id:
                    # Specific account query - determine target node
                    target_node = get_node_for_account(int(account_id))
                    
                    if target_node in online_nodes:
                        # Target node is online - query directly
                        st.info(f"🎯 Querying Node {target_node} (target partition for account {account_id})")
                        data = fetch_data(base_query, node=target_node, ttl=0)
                        combined_data = pd.concat([combined_data, data], ignore_index=True)
                        query_sources.append(f"Node {target_node} (partition)")
                    else:
                        # Target node is offline - check Node 1 (central) if available
                        st.warning(f"⚠️ Target Node {target_node} is offline for account {account_id}")
                        
                        if 1 in online_nodes and target_node != 1:
                            st.info("🔄 Searching Node 1 (central) as fallback...")
                            data = fetch_data(base_query, node=1, ttl=0)
                            combined_data = pd.concat([combined_data, data], ignore_index=True)
                            query_sources.append("Node 1 (central fallback)")
                        else:
                            st.error(f"❌ Cannot query account {account_id}: both Node {target_node} and Node 1 are offline")
                            
                else:
                    # Query all data - implement smart combination strategy
                    if 1 in online_nodes:
                        # Node 1 is online - it has complete data
                        st.info("📊 Node 1 online - querying complete central database")
                        data = fetch_data(query_without_limit, node=1, ttl=0)
                        combined_data = pd.concat([combined_data, data], ignore_index=True)
                        query_sources.append("Node 1 (complete)")
                        
                    else:
                        # Node 1 is offline - combine Node 2 and Node 3
                        st.warning("⚠️ Node 1 offline - combining partition nodes for complete view")
                        
                        for node in [2, 3]:
                            if node in online_nodes:
                                st.info(f"📊 Querying Node {node} partition data...")
                                data = fetch_data(query_without_limit, node=node, ttl=0)
                                combined_data = pd.concat([combined_data, data], ignore_index=True)
                                query_sources.append(f"Node {node} (partition)")
                        
                        if combined_data.empty:
                            st.error("❌ No partition nodes available - cannot retrieve complete data")
                
                # Remove duplicates and apply limit
                if not combined_data.empty:
                    # Remove duplicates based on trans_id (primary key)
                    initial_count = len(combined_data)
                    combined_data = combined_data.drop_duplicates(subset=['trans_id'], keep='first')
                    final_count = len(combined_data)
                    
                    if initial_count != final_count:
                        st.info(f"🔧 Removed {initial_count - final_count} duplicate records")
                    
                    # Sort by trans_id and apply limit
                    combined_data = combined_data.sort_values('trans_id').head(limit)
                    
            duration = time.time() - start_time
            
            if combined_data.empty:
                st.warning("⚠️ No data found matching your criteria")
            else:
                st.success(f"✅ Retrieved {len(combined_data)} rows in {duration:.3f}s")
                
                # Show data source information
                st.info(f"📡 Data sources: {', '.join(query_sources)}")
                
                st.dataframe(combined_data, use_container_width=True)
                
                # Show strategy details
                with st.expander("ℹ️ Query Strategy Details"):
                    st.write(f"**Query Strategy**: {'Single node' if len(query_sources) == 1 else 'Multi-node combination'}")
                    st.write(f"**Data Sources**: {', '.join(query_sources)}")
                    st.write(f"**Duration**: {duration:.3f}s")
                    st.write(f"**Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    st.write(f"**Offline Nodes**: {offline_nodes if offline_nodes else 'None'}")
                    st.write(f"**Records Retrieved**: {len(combined_data)}")
                    
                    if offline_nodes:
                        st.warning(f"⚠️ Some nodes were offline during this query: {offline_nodes}")
                        if 1 in offline_nodes and len(online_nodes) > 1:
                            st.info("✅ Complete data reconstructed from partition nodes")
                        elif 1 not in offline_nodes:
                            st.info("✅ Complete data available from central node")
                
        except Exception as e:
            st.error(f"❌ Error retrieving data: {str(e)}")
            st.error("Please check node connectivity and try again")