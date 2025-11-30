"""
View Transactions Page - Read Operations
"""

import streamlit as st
import time
from datetime import datetime
from python.db.db_config import fetch_data


def render(get_node_for_account, log_transaction):
    """Render the View Transactions page"""
    st.title("View Transactions (Read Operation)")

    st.markdown("""
    Browse transaction records from the database. The system will automatically 
    query the appropriate node based on your search criteria.
    """)

    # Configuration
    col1, col2 = st.columns(2)

    with col1:
        isolation_level = st.selectbox(
            "Isolation Level",
            ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"],
            index=1,
            help="Controls how the transaction sees concurrent changes"
        )

    with col2:
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

    # Build query based on filters
    base_query = "SELECT * FROM trans WHERE 1=1"

    # Determine which node to query
    if account_id:
        base_query += f" AND account_id = {account_id}"
        # Query specific partition node
        selected_node = get_node_for_account(int(account_id))
    else:
        # Query central node for all data
        selected_node = 1

    if trans_type != "All":
        base_query += f" AND type = '{trans_type}'"

    base_query += f" LIMIT {limit}"

    # Execute button
    if st.button("🔍 Fetch Data", type="primary"):
        start_time = time.time()

        try:
            with st.spinner(f"Querying database..."):
                # Execute query (node is automatically selected)
                data = fetch_data(base_query, node=selected_node)

            duration = time.time() - start_time

            # Log this READ operation
            log_transaction(
                operation='READ',
                query=base_query,
                node=selected_node,
                isolation_level=isolation_level,
                status='SUCCESS',
                duration=duration
            )

            if data.empty:
                st.warning("⚠️ No data found matching your criteria")
            else:
                st.success(f"✅ Retrieved {len(data)} rows in {duration:.3f}s")
                st.dataframe(data, use_container_width=True)

                # Show transaction info
                with st.expander("ℹ️ Transaction Details"):
                    st.write(f"**Isolation Level**: {isolation_level}")
                    st.write(f"**Duration**: {duration:.3f}s")
                    st.write(f"**Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    st.caption(f"System selected Node {selected_node} for this query")

        except Exception as e:
            duration = time.time() - start_time

            log_transaction(
                operation='READ',
                query=base_query,
                node=selected_node,
                isolation_level=isolation_level,
                status='FAILED',
                duration=duration
            )

            st.error(f"❌ Error: {str(e)}")

