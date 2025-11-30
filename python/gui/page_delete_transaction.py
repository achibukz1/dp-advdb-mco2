"""
Delete Transaction Page - Write Operations
"""

import streamlit as st
import time
from datetime import datetime
from python.db.db_config import fetch_data, execute_query


def render(get_node_for_account, log_transaction):
    """Render the Delete Transaction page"""
    st.title("Delete Transaction (Write Operation)")

    st.markdown("""
    Remove a transaction record from the database. The system will automatically 
    delete from all nodes.
    """)

    st.subheader("Find Transaction to Delete")

    col1, col2 = st.columns(2)

    with col1:
        trans_id = st.number_input("Transaction ID", min_value=1, value=1, key='delete_trans_id')

    with col2:
        isolation_level = st.selectbox(
            "Isolation Level",
            ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"],
            index=1,
            key='delete_isolation'
        )

    if st.button("🔍 Find Transaction"):
        try:
            search_query = f"SELECT * FROM trans WHERE trans_id = {trans_id}"
            found_data = fetch_data(search_query, node=1)

            if found_data.empty:
                st.warning(f"⚠️ Transaction ID {trans_id} not found")
            else:
                st.success(f"✅ Found transaction")
                st.dataframe(found_data)
                st.session_state.delete_trans = found_data.iloc[0].to_dict()
                st.session_state.delete_node = get_node_for_account(
                    int(st.session_state.delete_trans['account_id'])
                )
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")

    # Delete button (only show if transaction found)
    if 'delete_trans' in st.session_state:
        st.markdown("---")
        st.warning("⚠️ This action cannot be undone!")

        if st.button("🗑️ Delete Transaction", type="primary"):
            delete_query = f"DELETE FROM trans WHERE trans_id = {trans_id}"
            start_time = time.time()
            delete_node = st.session_state.delete_node

            try:
                with st.spinner("Deleting transaction..."):
                    # Delete from partition node
                    execute_query(delete_query, node=delete_node, isolation_level=isolation_level)

                    st.info("🔄 Replicating deletion to other nodes...")
                    time.sleep(0.5)

                    # Delete from central node
                    if delete_node != 1:
                        execute_query(delete_query, node=1, isolation_level=isolation_level)

                duration = time.time() - start_time

                log_transaction(
                    operation='DELETE',
                    query=delete_query,
                    node=delete_node,
                    isolation_level=isolation_level,
                    status='SUCCESS',
                    duration=duration
                )

                st.success(f"✅ Transaction deleted successfully in {duration:.3f}s")
                st.success("✅ Deletion replicated to all nodes")

                # Clear session state
                del st.session_state.delete_trans
                del st.session_state.delete_node

            except Exception as e:
                duration = time.time() - start_time

                log_transaction(
                    operation='DELETE',
                    query=delete_query,
                    node=delete_node,
                    isolation_level=isolation_level,
                    status='FAILED',
                    duration=duration
                )

                st.error(f"❌ Error: {str(e)}")

