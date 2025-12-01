# """
# Test Case #1 Page - Concurrent Read Transactions
# """
# import streamlit as st
# import pandas as pd
# import io
# import sys


# def render():
#     """Render the Test Case #1 page"""
#     st.title("📖 Test Case #1: Concurrent Read Transactions")

#     st.markdown("""
#     Run automated tests to simulate concurrent read transactions across multiple nodes.
#     This demonstrates that multiple transactions can read the same data simultaneously.
#     """)

#     # Configuration
#     st.header("⚙️ Test Configuration")

#     col1, col2, col3 = st.columns(3)

#     with col1:
#         isolation_level = st.selectbox(
#             "Isolation Level",
#             ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"],
#             index=1,
#             help="Controls how transactions see concurrent changes"
#         )

#     with col2:
#         num_transactions = st.slider(
#             "Number of Concurrent Transactions",
#             min_value=2,
#             max_value=20,
#             value=3,
#             help="How many transactions to run simultaneously"
#         )

#     with col3:
#         scenario = st.selectbox(
#             "Test Scenario",
#             [
#                 "Raw Reading",
#                 "Same Account Transactions",
#                 "Credit Transactions",
#                 "Date Range Query",
#                 "Account Analytics",
#                 "High-Value Transactions"
#             ]
#         )

#     # Map scenarios to queries
#     scenario_queries = {
#         "Raw Reading": "SELECT * FROM trans LIMIT 15000",
#         "Same Account Transactions": "SELECT * FROM trans WHERE account_id = 1 LIMIT 15000",
#         "Credit Transactions": "SELECT * FROM trans WHERE type = 'Credit' LIMIT 15000",
#         "Date Range Query": "SELECT * FROM trans WHERE newdate BETWEEN '1995-01-01' AND '1995-12-31' LIMIT 15000",
#         "Account Analytics": "SELECT account_id, COUNT(*) as trans_count, SUM(amount) as total_amount FROM trans GROUP BY account_id LIMIT 15000",
#         "High-Value Transactions": "SELECT * FROM trans WHERE amount > 10000 ORDER BY amount DESC LIMIT 15000"
#     }

#     query = scenario_queries[scenario]

#     # Show query
#     with st.expander("📝 View SQL Query"):
#         st.code(query, language="sql")

#     # Run test button
#     if st.button("🚀 Run Test", type="primary", use_container_width=True):
#         # Import test class
#         try:
#             from python.case1_test import SimpleConcurrentReadTest

#             # Initialize test
#             test = SimpleConcurrentReadTest()

#             # Progress indicator
#             progress_text = st.empty()
#             progress_bar = st.progress(0)

#             progress_text.text(f"Initializing {num_transactions} concurrent transactions...")
#             progress_bar.progress(20)

#             # Run test (suppress print statements)
#             old_stdout = sys.stdout
#             sys.stdout = io.StringIO()

#             try:
#                 results = test.run_test(
#                     query=query,
#                     num_transactions=num_transactions,
#                     isolation_level=isolation_level
#                 )

#                 # Calculate metrics
#                 metrics = test.calculate_metrics()

#             finally:
#                 sys.stdout = old_stdout

#             progress_bar.progress(100)
#             progress_text.text("✅ Test completed!")

#             # Display results in tabs
#             tab1, tab2, tab3 = st.tabs(["📊 Summary", "⏱️ Timeline", "📈 Analysis"])

#             with tab1:
#                 st.subheader("Test Summary")

#                 # Create summary table
#                 summary_data = []
#                 for txn_id, result in sorted(results.items()):
#                     summary_data.append({
#                         'Transaction': txn_id,
#                         'Node': result['node'],
#                         'Status': '✅ Success' if result['status'] == 'SUCCESS' else '❌ Failed',
#                         'Rows Read': result.get('rows_read', 'N/A'),
#                         'Duration (s)': f"{result['duration']:.6f}"
#                     })

#                 df = pd.DataFrame(summary_data)
#                 st.dataframe(df, use_container_width=True, hide_index=True)

#                 # Metrics
#                 col1, col2, col3 = st.columns(3)

#                 with col1:
#                     st.metric("Success Rate", f"{metrics['success_rate']:.2f}%")
#                 with col2:
#                     st.metric("Throughput", f"{metrics['throughput']:.6f} txn/s")
#                 with col3:
#                     st.metric("Avg Response Time", f"{metrics['avg_response_time']:.6f}s")

#             with tab2:
#                 st.subheader("Transaction Timeline")

#                 # Timeline visualization
#                 timeline_data = []
#                 for txn_id, result in sorted(results.items()):
#                     timeline_data.append({
#                         'Transaction': txn_id,
#                         'Duration': result['duration']
#                     })

#                 df_timeline = pd.DataFrame(timeline_data)
#                 st.bar_chart(df_timeline.set_index('Transaction')['Duration'])

#                 st.info("""
#                 **Interpretation**: 
#                 - Similar bar lengths (~2s each) = Concurrent execution ✅
#                 - One bar much longer = Sequential execution ❌
#                 """)

#             with tab3:
#                 st.subheader("Concurrency Analysis")

#                 col1, col2 = st.columns(2)

#                 with col1:
#                     st.metric("Total Execution Time", f"{metrics['total_time']:.6f}s")
#                     sequential_time = sum(r['duration'] for r in results.values())
#                     st.metric("If Run Sequentially", f"{sequential_time:.6f}s")

#                 with col2:
#                     speedup = sequential_time / metrics['total_time'] if metrics['total_time'] > 0 else 1
#                     st.metric("Speedup Factor", f"{speedup:.2f}x")

#                     if speedup > 2:
#                         st.success("✅ Excellent concurrency!")
#                     elif speedup > 1.5:
#                         st.info("ℹ️ Good concurrency")
#                     else:
#                         st.warning("⚠️ Limited concurrency")

#                 # Data consistency check
#                 st.markdown("---")
#                 st.subheader("Data Consistency Check")

#                 successful_reads = [r for r in results.values() if r['status'] == 'SUCCESS']
#                 if successful_reads:
#                     row_counts = [r['rows_read'] for r in successful_reads]
#                     if len(set(row_counts)) == 1:
#                         st.success(f"✅ CONSISTENT: All transactions read {row_counts[0]} rows")
#                     else:
#                         st.warning(f"⚠️ Row counts vary: {set(row_counts)}")
#                         st.info("Note: Different nodes may have different data partitions")

#         except ImportError as e:
#             st.error(f"❌ Error importing test module: {str(e)}")
#         except Exception as e:
#             st.error(f"❌ Test failed: {str(e)}")
#             st.exception(e)

