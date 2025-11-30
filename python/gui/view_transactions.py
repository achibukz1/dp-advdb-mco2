"""
View Transactions Page - Read Operation
"""
import streamlit as st
import pandas as pd
import time
from datetime import datetime


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
        view_transactions = [t for t in st.session_state.active_transactions if t.get('page') == 'view']
        if view_transactions:
            try:
                committed_count = 0
                indices_to_remove = []

                # Collect indices and commit transactions
                for txn in view_transactions:
                    idx = st.session_state.active_transactions.index(txn)
                    indices_to_remove.append(idx)

                    conn = st.session_state.transaction_connections[idx]
                    cursor = st.session_state.transaction_cursors[idx]

                    # Commit the transaction
                    conn.commit()
                    cursor.close()
                    conn.close()

                    # Log the transaction
                    duration = time.time() - txn['start_time']
                    log_transaction(
                        operation=txn['operation'],
                        query=txn['query'],
                        node=txn['node'],
                        isolation_level=txn['isolation_level'],
                        status='SUCCESS',
                        duration=duration
                    )
                    committed_count += 1

                # Remove in reverse order to maintain correct indices
                for idx in sorted(indices_to_remove, reverse=True):
                    del st.session_state.active_transactions[idx]
                    del st.session_state.transaction_connections[idx]
                    del st.session_state.transaction_cursors[idx]

                st.success(f"✅ {committed_count} transaction(s) committed!")
                st.toast(f"{committed_count} transaction(s) committed successfully")
            except Exception as e:
                st.error(f"Commit failed: {str(e)}")
        else:
            st.warning("No active READ transaction to commit")

    if rollback_button:
        view_transactions = [t for t in st.session_state.active_transactions if t.get('page') == 'view']
        if view_transactions:
            try:
                rolled_back_count = 0
                indices_to_remove = []

                # Collect indices and rollback transactions
                for txn in view_transactions:
                    idx = st.session_state.active_transactions.index(txn)
                    indices_to_remove.append(idx)

                    conn = st.session_state.transaction_connections[idx]
                    cursor = st.session_state.transaction_cursors[idx]
                    conn.rollback()
                    cursor.close()
                    conn.close()
                    rolled_back_count += 1

                # Remove in reverse order to maintain correct indices
                for idx in sorted(indices_to_remove, reverse=True):
                    del st.session_state.active_transactions[idx]
                    del st.session_state.transaction_connections[idx]
                    del st.session_state.transaction_cursors[idx]

                st.info(f"↩️ {rolled_back_count} transaction(s) rolled back - no changes logged")
                st.toast(f"{rolled_back_count} transaction(s) rolled back")
            except Exception as e:
                st.error(f"Rollback failed: {str(e)}")
        else:
            st.warning("No active READ transaction to rollback")

    if fetch_button:
        start_time = time.time()

        try:
            from python.db.db_config import create_dedicated_connection

            with st.spinner(f"Starting transaction on Node {selected_node}..."):
                # Create dedicated connection and start transaction
                conn = create_dedicated_connection(selected_node, isolation_level)
                cursor = conn.cursor(dictionary=True)

                # Set isolation level and start transaction
                cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation_level}")
                cursor.execute("START TRANSACTION")

                # Execute query
                cursor.execute(base_query)
                results = cursor.fetchall()
                data = pd.DataFrame(results)

                # Append connection and transaction to lists
                st.session_state.transaction_connections.append(conn)
                st.session_state.transaction_cursors.append(cursor)
                st.session_state.active_transactions.append({
                    'page': 'view',
                    'node': selected_node,
                    'operation': 'READ',
                    'query': base_query,
                    'isolation_level': isolation_level,
                    'start_time': start_time,
                    'data': data.copy()  # Store the fetched data
                })

            duration = time.time() - start_time

            # DON'T log yet - will log when user commits

            if data.empty:
                st.warning("⚠️ No data found matching your criteria")
            else:
                st.success(f"✅ Retrieved {len(data)} rows in {duration:.3f}s")
                st.warning("⏳ Transaction active - Click 'Commit' to finalize or 'Rollback' to cancel")
                st.dataframe(data, use_container_width=True)

                # Show transaction info
                with st.expander("ℹ️ Transaction Details"):
                    st.write(f"**Isolation Level**: {isolation_level}")
                    st.write(f"**Duration**: {duration:.3f}s")
                    st.write(f"**Timestamp**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    st.write(f"**Transaction Status**: ACTIVE (not committed)")
                    st.caption(f"System selected Node {selected_node} for this query")

        except Exception as e:
            # Don't log failed transactions - only log successful commits
            st.error(f"❌ Error: {str(e)}")