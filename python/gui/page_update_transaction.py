"""
Update Transaction Page - Write Operations
"""

import streamlit as st
import time
from datetime import datetime
from python.db.db_config import fetch_data, execute_query


def render(get_node_for_account, log_transaction):
    """Render the Update Transaction page"""
    st.title("Update Transaction (Write Operation)")

    st.markdown("""
    Modify an existing transaction record. The system will automatically find 
    and update the record across all nodes.
    """)

    # Step 1: Find the transaction to update
    st.subheader("Step 1: Find Transaction to Update")

    col1, col2 = st.columns(2)

    with col1:
        trans_id = st.number_input("Transaction ID", min_value=1, value=1)

    with col2:
        st.info("System will search across all nodes automatically")

    if st.button("🔍 Search"):
        try:
            # Try to find transaction (search central node first)
            search_query = f"SELECT * FROM trans WHERE trans_id = {trans_id}"
            found_data = fetch_data(search_query, node=1)

            if found_data.empty:
                st.warning(f"⚠️ Transaction ID {trans_id} not found")
            else:
                st.success(f"✅ Found transaction")
                st.dataframe(found_data)

                # Store in session state for update form
                st.session_state.current_trans = found_data.iloc[0].to_dict()
                # Determine which node this account belongs to
                st.session_state.update_node = get_node_for_account(
                    int(st.session_state.current_trans['account_id'])
                )

        except Exception as e:
            st.error(f"❌ Error: {str(e)}")

    # Step 2: Update form (only show if transaction found)
    if 'current_trans' in st.session_state:
        st.markdown("---")
        st.subheader("Step 2: Update Values")

        current = st.session_state.current_trans

        col1, col2 = st.columns(2)

        with col1:
            new_amount = st.number_input("New Amount",
                                        value=float(current['amount']),
                                        step=100.0)
            new_type = st.selectbox("New Type",
                                   ["Credit", "Debit"],
                                   index=0 if current['type'] == 'Credit' else 1)

        with col2:
            new_operation = st.text_input("New Operation",
                                         value=current['operation'])
            isolation_level = st.selectbox(
                "Isolation Level",
                ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"],
                index=1,
                key='update_isolation'
            )

        if st.button("💾 Update Transaction", type="primary"):
            update_query = f"""
            UPDATE trans 
            SET amount = {new_amount}, 
                type = '{new_type}', 
                operation = '{new_operation}'
            WHERE trans_id = {trans_id}
            """

            start_time = time.time()
            update_node = st.session_state.update_node

            try:
                with st.spinner(f"Updating transaction on Node {update_node}..."):
                    # Update on partition node
                    result = execute_query(update_query, node=update_node,
                                isolation_level=isolation_level)

                with st.spinner("🔄 Replicating update to central node..."):
                    time.sleep(0.3)

                    # Update central node if different
                    if update_node != 1:
                        execute_query(update_query, node=1, isolation_level=isolation_level)

                duration = time.time() - start_time

                log_transaction(
                    operation='UPDATE',
                    query=update_query,
                    node=update_node,
                    isolation_level=isolation_level,
                    status='SUCCESS',
                    duration=duration
                )

                st.success(f"✅ Transaction updated successfully in {duration:.3f}s")
                st.success("✅ Replicated to other nodes")

                # Clear session state
                del st.session_state.current_trans
                del st.session_state.update_node

            except Exception as e:
                duration = time.time() - start_time

                log_transaction(
                    operation='UPDATE',
                    query=update_query,
                    node=update_node,
                    isolation_level=isolation_level,
                    status='FAILED',
                    duration=duration
                )

                st.error(f"❌ Error: {str(e)}")

