"""
Add Transaction Page - Insert/Write Operations
"""

import streamlit as st
import time
from datetime import datetime
from python.db.db_config import fetch_data, execute_query


def render(get_node_for_account, log_transaction):
    """Render the Add Transaction page"""
    st.title("Add New Transaction (Write Operation)")

    st.markdown("""
    Insert a new transaction record. The system will automatically route it to the 
    appropriate node and handle replication.
    """)

    # Configuration
    isolation_level = st.selectbox(
        "Isolation Level",
        ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"],
        index=1,
        key='insert_isolation',
        help="Controls transaction isolation level"
    )

    # Transaction form
    st.subheader("Transaction Details")

    col1, col2 = st.columns(2)

    with col1:
        account_id = st.number_input("Account ID", min_value=1, value=1)
        trans_date = st.date_input("Transaction Date")
        trans_type = st.selectbox("Type", ["Credit", "Debit"])

    with col2:
        operation = st.text_input("Operation", placeholder="e.g., Credit in Cash")
        amount = st.number_input("Amount", min_value=0.0, value=1000.0, step=100.0)
        k_symbol = st.text_input("K Symbol", value="")

    # Insert button
    if st.button("💾 Insert Transaction", type="primary"):

        # Determine target node based on account_id
        target_node = get_node_for_account(account_id)

        # Build INSERT query
        insert_query = f"""
        INSERT INTO trans (account_id, newdate, type, operation, amount, k_symbol)
        VALUES ({account_id}, '{trans_date}', '{trans_type}', '{operation}', {amount}, '{k_symbol}')
        """

        start_time = time.time()

        try:
            with st.spinner(f"Inserting transaction..."):
                # Execute insert on partition node
                result = execute_query(insert_query, node=target_node,
                                     isolation_level=isolation_level)

                # Simulate replication to other nodes
                st.info("🔄 Replicating to other nodes...")
                time.sleep(0.5)  # Simulate replication delay

                # Replicate to Node 1 (central node)
                if target_node != 1:
                    execute_query(insert_query, node=1, isolation_level=isolation_level)

                # If insert was on Node 1, replicate to partition node
                if target_node == 1:
                    replica_node = get_node_for_account(account_id)
                    if replica_node != 1:
                        execute_query(insert_query, node=replica_node, isolation_level=isolation_level)

            duration = time.time() - start_time

            # Log the WRITE operation
            log_transaction(
                operation='INSERT',
                query=insert_query,
                node=target_node,
                isolation_level=isolation_level,
                status='SUCCESS',
                duration=duration
            )

            st.success(f"✅ Transaction inserted successfully in {duration:.3f}s")
            st.success("✅ Replicated to other nodes")

            # Show inserted data
            with st.expander("📝 View Inserted Record"):
                verify_query = f"SELECT * FROM trans WHERE account_id = {account_id} ORDER BY trans_id DESC LIMIT 1"
                inserted_data = fetch_data(verify_query, node=target_node)
                st.dataframe(inserted_data)
                st.caption(f"Data inserted into Node {target_node} and replicated to central node")

        except Exception as e:
            duration = time.time() - start_time

            log_transaction(
                operation='INSERT',
                query=insert_query,
                node=target_node,
                isolation_level=isolation_level,
                status='FAILED',
                duration=duration
            )

            st.error(f"❌ Error: {str(e)}")

