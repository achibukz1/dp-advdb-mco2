# """
# Transaction Log Module - Old Logic
# This module contains the original logic for viewing and analyzing transaction logs.
# """

# import streamlit as st
# import pandas as pd


# def render():
#     """
#     Render the Transaction Log page with the old logic.
#     """
#     st.title("Transaction Log & Concurrency Analysis")

#     st.markdown("""
#     View all operations performed and analyze concurrent transactions.
#     **This shows which test cases occurred naturally during usage.**
#     """)

#     if not st.session_state.transaction_log:
#         st.info("ℹ️ No transactions logged yet. Perform some operations first!")
#     else:
#         # Display log
#         log_df = pd.DataFrame(st.session_state.transaction_log)

#         st.subheader("All Transactions")
#         st.dataframe(log_df, use_container_width=True)

#         # Analyze concurrency
#         st.markdown("---")
#         st.subheader("🔍 Concurrency Analysis")

#         # Find concurrent operations
#         log_df['timestamp'] = pd.to_datetime(log_df['timestamp'])
#         log_df = log_df.sort_values('timestamp')

#         # Detect overlapping transactions (within 5 seconds = concurrent)
#         st.markdown("### Detected Concurrent Operations")

#         concurrent_found = False
#         for i in range(len(log_df) - 1):
#             time_diff = (log_df.iloc[i+1]['timestamp'] - log_df.iloc[i]['timestamp']).total_seconds()

#             if time_diff < 5:  # Within 5 seconds = concurrent
#                 concurrent_found = True
#                 op1 = log_df.iloc[i]
#                 op2 = log_df.iloc[i+1]

#                 # Determine test case
#                 if op1['operation'] == 'READ' and op2['operation'] == 'READ':
#                     case = "📖 Case #1: Concurrent Reads"
#                     color = "blue"
#                 elif (op1['operation'] == 'READ' and op2['operation'] in ['INSERT', 'UPDATE', 'DELETE']) or \
#                      (op2['operation'] == 'READ' and op1['operation'] in ['INSERT', 'UPDATE', 'DELETE']):
#                     case = "🔄 Case #2: Read-Write Conflict"
#                     color = "orange"
#                 elif op1['operation'] in ['INSERT', 'UPDATE', 'DELETE'] and op2['operation'] in ['INSERT', 'UPDATE', 'DELETE']:
#                     case = "✍️ Case #3: Write-Write Conflict"
#                     color = "red"

#                 with st.container(border=True):
#                     st.markdown(f"**{case}**")
#                     col1, col2 = st.columns(2)

#                     with col1:
#                         st.write(f"**Operation 1**: {op1['operation']}")
#                         st.write(f"Node: {op1['node']}")
#                         st.write(f"Time: {op1['timestamp']}")

#                     with col2:
#                         st.write(f"**Operation 2**: {op2['operation']}")
#                         st.write(f"Node: {op2['node']}")
#                         st.write(f"Time: {op2['timestamp']}")

#                     st.write(f"⏱️ Time difference: {time_diff:.2f}s")

#         if not concurrent_found:
#             st.info("ℹ️ No concurrent operations detected yet. Try performing operations within 5 seconds of each other.")
