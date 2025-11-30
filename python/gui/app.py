"""
STADVDB MCO2: Distributed Database Transaction Manager
Users perform CRUD operations; System logs concurrent transactions automatically
"""

import streamlit as st
import pandas as pd
import time
from datetime import datetime
import json

from python.db.db_config import fetch_data, execute_query

# Import page modules
from python.gui import (
    page_view_transactions,
    page_add_transaction,
    page_update_transaction,
    page_delete_transaction,
    page_transaction_log,
    page_test_case1
)

st.set_page_config(
    page_title="Transaction Manager",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state for transaction tracking
if 'transaction_log' not in st.session_state:
    st.session_state.transaction_log = []

# Sidebar
st.sidebar.title("Database Operations")
st.sidebar.text("STADVDB MCO2 Group 12")
st.sidebar.text("Balcita, Bukuhan, Cu, Dimaunahan")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Select Operation",
    [
        "Home",
        "View Transactions",
        "Add Transaction",
        "Update Transaction",
        "Delete Transaction",
        "Transaction Log",
        "Test Case #1"
    ]
)

# Helper function to determine node based on account_id
def get_node_for_account(account_id):
    """
    Determine which node to use based on account_id
    Node 1: Central (all data)
    Node 2: Even account_id
    Node 3: Odd account_id
    """
    if account_id % 2 == 0:
        return 2
    else:
        return 3

# Helper function to log transactions
def log_transaction(operation, query, node, isolation_level, status, duration):
    """Log transaction for later analysis"""
    log_entry = {
        'timestamp': datetime.now().isoformat(),
        'operation': operation,
        'query': query,
        'node': node,
        'isolation_level': isolation_level,
        'status': status,
        'duration': duration,
        'user_session': st.session_state.get('user_id', 'anonymous')
    }

    st.session_state.transaction_log.append(log_entry)

    # Also save to file for persistence
    with open('transaction_log.json', 'a') as f:
        f.write(json.dumps(log_entry) + '\n')

def main():
    # ============================================================================
    # HOME PAGE
    # ============================================================================
    if page == "Home":
        st.title("Distributed Database Transaction Manager")

        st.markdown("""
        ## Welcome to the Transaction Manager
        
        INSERT INTRO HERE
                    
        """)

        # Show node status
        st.markdown("---")
        st.subheader("📊 Current Node Status")

        col1, col2, col3 = st.columns(3)

        try:
            node1_count = fetch_data("SELECT COUNT(*) as count FROM trans", node=1)['count'][0]
            node2_count = fetch_data("SELECT COUNT(*) as count FROM trans", node=2)['count'][0]
            node3_count = fetch_data("SELECT COUNT(*) as count FROM trans", node=3)['count'][0]

            with col1:
                st.metric("Node 1 (Central)", "✅ Active", f"{node1_count:,} rows")
            with col2:
                st.metric("Node 2 (Even Accounts)", "✅ Active", f"{node2_count:,} rows")
            with col3:
                st.metric("Node 3 (Odd Accounts)", "✅ Active", f"{node3_count:,} rows")
        except Exception as e:
            st.error(f"⚠️ Database connection issue: {str(e)}")

    # ============================================================================
    # PAGE ROUTING - Delegate to page modules
    # ============================================================================
    elif page == "View Transactions":
        page_view_transactions.render(get_node_for_account, log_transaction)

    elif page == "Add Transaction":
        page_add_transaction.render(get_node_for_account, log_transaction)

    elif page == "Update Transaction":
        page_update_transaction.render(get_node_for_account, log_transaction)

    elif page == "Delete Transaction":
        page_delete_transaction.render(get_node_for_account, log_transaction)

    elif page == "Transaction Log":
        page_transaction_log.render(get_node_for_account, log_transaction)

    elif page == "Test Case #1":
        page_test_case1.render(get_node_for_account, log_transaction)

if __name__ == "__main__":
    main()

