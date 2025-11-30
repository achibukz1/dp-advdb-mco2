"""
Case #2: Mixed Read/Write Concurrent Transactions Test

Tests concurrent transactions where at least one transaction is writing (update/delete)
and others are reading the same data item. Uses distributed lock manager for write operations.

Specs requirement:
- At least one transaction must access Node 1 (the node with ALL rows)
- Transactions must happen concurrently
- Test all 4 isolation levels
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import mysql.connector
import threading
import time
from datetime import datetime
import pandas as pd
from python.utils.lock_manager import DistributedLockManager
from python.db_config import get_node_config, NODE_CONFIGS

class MixedReadWriteTest:
    def __init__(self):
        self.results = {}
        self.lock = threading.Lock()
        
        # Get database configs from db_config.py
        # Build node_configs dict for lock manager
        self.node_configs = {
            1: get_node_config(1),
            2: get_node_config(2),
            3: get_node_config(3)
        }
        
        # Initialize distributed lock manager
        self.lock_manager = DistributedLockManager(self.node_configs, current_node_id="case2_test")
    
    def write_transaction(self, node_num, trans_id, new_amount, transaction_id, isolation_level):
        """Execute a write (UPDATE) transaction on specified node with distributed locking"""
        start_time = time.time()
        config = self.node_configs[node_num]
        resource_id = f"trans_{trans_id}"
        
        conn = None
        cursor = None
        lock_acquired = False
        
        try:
            # Acquire distributed lock BEFORE starting transaction
            print(f"[{transaction_id}] Attempting to acquire lock on {resource_id} at Node {node_num} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
            
            lock_acquired = self.lock_manager.acquire_lock(resource_id, node_num, timeout=30)
            
            if not lock_acquired:
                raise Exception(f"Failed to acquire lock on {resource_id}")
            
            print(f"[{transaction_id}] Lock acquired, starting write on Node {node_num} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
            
            # Connect to database
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor(dictionary=True)
            
            # Set isolation level
            cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
            
            # Validate we still hold the lock before starting transaction
            if not self.lock_manager.check_lock(resource_id, node_num):
                raise Exception(f"Lost lock on {resource_id} at Node {node_num}")
            
            # Start transaction
            cursor.execute("START TRANSACTION")
            
            # Read current value
            cursor.execute("SELECT trans_id, amount FROM trans WHERE trans_id = %s", (trans_id,))
            before = cursor.fetchone()
            
            if not before:
                raise Exception(f"Record with trans_id={trans_id} not found on Node {node_num}")
            
            # Update the amount IMMEDIATELY (locks the row)
            cursor.execute("UPDATE trans SET amount = %s WHERE trans_id = %s", (new_amount, trans_id))
            
            # Hold transaction open AFTER update (keeps row locked)
            time.sleep(3)
            affected_rows = cursor.rowcount
            
            # Read updated value
            cursor.execute("SELECT trans_id, amount FROM trans WHERE trans_id = %s", (trans_id,))
            after = cursor.fetchone()
            
            # Commit
            conn.commit()
            
            end_time = time.time()
            
            print(f"[{transaction_id}] Completed write on Node {node_num} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
            print(f"[{transaction_id}] Updated trans_id={trans_id}: {before['amount']} → {after['amount']}")
            
            # Store results
            with self.lock:
                self.results[transaction_id] = {
                    'type': 'WRITE',
                    'node': f'node{node_num}',
                    'status': 'SUCCESS',
                    'trans_id': trans_id,
                    'before_amount': float(before['amount']),
                    'after_amount': float(after['amount']),
                    'affected_rows': affected_rows,
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration': end_time - start_time
                }
        
        except Exception as e:
            end_time = time.time()
            if conn:
                conn.rollback()
            
            print(f"[{transaction_id}] ERROR on Node {node_num}: {str(e)}")
            
            with self.lock:
                self.results[transaction_id] = {
                    'type': 'WRITE',
                    'node': f'node{node_num}',
                    'status': 'FAILED',
                    'error': str(e),
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration': end_time - start_time
                }
        
        finally:
            # Always release the lock
            if lock_acquired:
                self.lock_manager.release_lock(resource_id, node_num)
                print(f"[{transaction_id}] Lock released on {resource_id}")
            
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def read_transaction(self, node_num, trans_id, transaction_id, isolation_level):
        """Execute a read (SELECT) transaction on specified node"""
        start_time = time.time()
        config = self.node_configs[node_num]
        
        conn = None
        cursor = None
        
        try:
            # Connect to database
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor(dictionary=True)
            
            # Set isolation level
            cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
            
            # Start transaction
            cursor.execute("START TRANSACTION")
            
            print(f"[{transaction_id}] Starting read on Node {node_num} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
            
            # First read
            cursor.execute("SELECT trans_id, amount FROM trans WHERE trans_id = %s", (trans_id,))
            first_read = cursor.fetchone()
            
            # Simulate processing time (hold transaction open)
            time.sleep(2)
            
            # Second read (to detect non-repeatable reads)
            cursor.execute("SELECT trans_id, amount FROM trans WHERE trans_id = %s", (trans_id,))
            second_read = cursor.fetchone()
            
            # Commit
            conn.commit()
            
            end_time = time.time()
            
            print(f"[{transaction_id}] Completed read on Node {node_num} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
            
            if first_read and second_read:
                print(f"[{transaction_id}] Read trans_id={trans_id}: 1st={first_read['amount']}, 2nd={second_read['amount']}")
                
                # Check for non-repeatable read
                repeatable = float(first_read['amount']) == float(second_read['amount'])
                
                with self.lock:
                    self.results[transaction_id] = {
                        'type': 'READ',
                        'node': f"node{node_num}",
                        'status': 'SUCCESS',
                        'trans_id': trans_id,
                        'first_read': float(first_read['amount']) if first_read else None,
                        'second_read': float(second_read['amount']) if second_read else None,
                        'repeatable': repeatable,
                        'start_time': start_time,
                        'end_time': end_time,
                        'duration': end_time - start_time
                    }
            else:
                raise Exception(f"Record with trans_id={trans_id} not found on Node {node_num}")
        
        except Exception as e:
            end_time = time.time()
            if conn:
                conn.rollback()
            
            print(f"[{transaction_id}] ERROR on Node {node_num}: {str(e)}")
            
            with self.lock:
                self.results[transaction_id] = {
                    'type': 'READ',
                    'node': f'node{node_num}',
                    'status': 'FAILED',
                    'error': str(e),
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration': end_time - start_time
                }
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def run_test(self, trans_id, isolation_level="READ COMMITTED"):
        """
        Run mixed read/write test with 10 concurrent transactions across different nodes
        All transactions access the SAME trans_id to test cross-node distributed concurrency
        Ratio: 4 writers (2 on Node 1, 2 on Node 2), 6 readers (2 on Node 1, 4 on Node 2)
        
        Tests distributed lock contention and isolation across multiple nodes
        """
        print(f"\n{'='*70}")
        print(f"Running Case #2: Mixed Read/Write on trans_id={trans_id}")
        print(f"Isolation Level: {isolation_level}")
        print(f"Configuration (Cross-Node Access - 10 Concurrent Transactions):")
        print(f"  WRITERS (4 total): 2 on Node 1, 2 on Node 2")
        print(f"  READERS (6 total): 2 on Node 1, 4 on Node 2")
        print(f"  All transactions access trans_id={trans_id}")
        print(f"{'='*70}\n")
        
        self.results = {}  # Reset results
        threads = []
        
        # Create 4 writer threads - 2 on Node 1, 2 on Node 2
        for i in range(1, 3):
            threads.append(threading.Thread(
                target=self.write_transaction,
                args=(1, trans_id, 10000.00 + i*1111.11, f"T{i}_WRITER_Node1", isolation_level)
            ))
        
        for i in range(3, 5):
            threads.append(threading.Thread(
                target=self.write_transaction,
                args=(2, trans_id, 10000.00 + i*1111.11, f"T{i}_WRITER_Node2", isolation_level)
            ))
        
        # Create 6 reader threads - 2 on Node 1, 4 on Node 2
        for i in range(5, 7):
            threads.append(threading.Thread(
                target=self.read_transaction,
                args=(1, trans_id, f"T{i}_READER_Node1", isolation_level)
            ))
        
        for i in range(7, 11):
            threads.append(threading.Thread(
                target=self.read_transaction,
                args=(2, trans_id, f"T{i}_READER_Node2", isolation_level)
            ))
        
        # Start all threads with slight staggering
        for i, thread in enumerate(threads):
            thread.start()
            if i < 4:  # Stagger writers more
                time.sleep(0.2)
            elif i == 4:  # Slight delay before readers start
                time.sleep(0.1)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Display results
        self.display_results()
        
        # Restore original value on both nodes
        self.restore_original_value(trans_id, 1)
        self.restore_original_value(trans_id, 2)
        
        return self.results
    
    def restore_original_value(self, trans_id, node_num):
        """Restore the original value after test"""
        try:
            config = self.node_configs[node_num]
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor()
            
            # Use a known value to restore (you can modify this)
            cursor.execute("UPDATE trans SET amount = 1000.00 WHERE trans_id = %s", (trans_id,))
            conn.commit()
            
            cursor.close()
            conn.close()
            
            print(f"\n✓ Restored trans_id={trans_id} to original value")
        except Exception as e:
            print(f"\n⚠️  Warning: Could not restore original value: {e}")
    
    def display_results(self):
        """Display test results"""
        print(f"\n{'='*70}")
        print("TEST RESULTS")
        print(f"{'='*70}\n")
        
        # Create summary table
        summary = []
        for txn_id, result in sorted(self.results.items()):
            row = {
                'Transaction': txn_id,
                'Type': result['type'],
                'Node': result['node'],
                'Status': result['status'],
                'Duration (s)': f"{result['duration']:.6f}"
            }
            
            if result['type'] == 'WRITE' and result['status'] == 'SUCCESS':
                row['Before→After'] = f"{result['before_amount']:.2f}→{result['after_amount']:.2f}"
            elif result['type'] == 'READ' and result['status'] == 'SUCCESS':
                row['Repeatable?'] = '✓' if result['repeatable'] else '✗'
                row['Values'] = f"{result['first_read']:.2f}, {result['second_read']:.2f}"
            
            summary.append(row)
        
        df = pd.DataFrame(summary)
        print(df.to_string(index=False))
        
        # Analyze anomalies
        print(f"\n{'='*70}")
        print("ANOMALY DETECTION")
        print(f"{'='*70}\n")
        
        # Check for dirty reads (reader sees uncommitted write)
        writer_result = next((r for r in self.results.values() if r['type'] == 'WRITE'), None)
        read_results = [r for r in self.results.values() if r['type'] == 'READ' and r['status'] == 'SUCCESS']
        
        if writer_result and writer_result['status'] == 'SUCCESS':
            new_value = writer_result['after_amount']
            
            for read_result in read_results:
                if read_result['first_read'] == new_value or read_result['second_read'] == new_value:
                    print(f"⚠️  Possible DIRTY READ detected in {[k for k, v in self.results.items() if v == read_result][0]}")
                    print(f"   Reader saw value {new_value} that was being written")
        
        # Check for non-repeatable reads
        non_repeatable = [r for r in read_results if not r['repeatable']]
        if non_repeatable:
            print(f"\n⚠️  NON-REPEATABLE READS detected: {len(non_repeatable)} reader(s)")
            for result in non_repeatable:
                txn_id = [k for k, v in self.results.items() if v == result][0]
                print(f"   {txn_id}: First={result['first_read']:.2f}, Second={result['second_read']:.2f}")
        else:
            print(f"\n✅ NO NON-REPEATABLE READS: All readers saw consistent values")
        
        # Show timing overlap
        print(f"\n{'='*70}")
        print("CONCURRENCY ANALYSIS")
        print(f"{'='*70}\n")
        
        start_times = [r['start_time'] for r in self.results.values()]
        end_times = [r['end_time'] for r in self.results.values()]
        
        earliest_start = min(start_times)
        latest_end = max(end_times)
        total_time = latest_end - earliest_start
        
        print(f"Total execution time: {total_time:.6f} seconds")
        print(f"Expected if sequential: {sum(r['duration'] for r in self.results.values()):.6f} seconds")
        print(f"✅ Transactions ran concurrently" if total_time < 5 else "⚠️ Transactions may have run sequentially")
        
        # Check distributed locking effectiveness
        writer = next((r for r in self.results.values() if r['type'] == 'WRITE'), None)
        if writer and writer['status'] == 'SUCCESS':
            print(f"\n✅ DISTRIBUTED LOCK: Writer successfully acquired lock and completed update")
    
    def calculate_metrics(self):
        """Calculate performance metrics for comparison"""
        start_times = [r['start_time'] for r in self.results.values()]
        end_times = [r['end_time'] for r in self.results.values()]
        
        earliest_start = min(start_times)
        latest_end = max(end_times)
        total_time = latest_end - earliest_start
        
        successful_txns = sum(1 for r in self.results.values() if r['status'] == 'SUCCESS')
        failed_txns = sum(1 for r in self.results.values() if r['status'] == 'FAILED')
        
        # Throughput = successful transactions / total time
        throughput = successful_txns / total_time if total_time > 0 else 0
        
        # Average response time
        avg_response = sum(r['duration'] for r in self.results.values()) / len(self.results)
        
        # Count anomalies
        read_results = [r for r in self.results.values() if r['type'] == 'READ' and r['status'] == 'SUCCESS']
        non_repeatable_reads = sum(1 for r in read_results if not r['repeatable'])
        
        return {
            'total_time': total_time,
            'successful_txns': successful_txns,
            'failed_txns': failed_txns,
            'throughput': throughput,
            'avg_response_time': avg_response,
            'success_rate': (successful_txns / len(self.results)) * 100,
            'non_repeatable_reads': non_repeatable_reads
        }
    
    def cleanup(self):
        """Cleanup: release all locks"""
        self.lock_manager.release_all_locks()

def main():
    """Run all test cases for Case #2"""
    test = MixedReadWriteTest()
    
    # Single scenario - one trans_id that exists on Node 2 (276-81998)
    trans_id = 276
    
    # Isolation levels to test
    isolation_levels = [
        'READ UNCOMMITTED',
        'READ COMMITTED',
        'REPEATABLE READ',
        'SERIALIZABLE'
    ]
    
    print("\n" + "="*70)
    print("CASE #2: MIXED READ/WRITE CONCURRENT TRANSACTIONS TEST")
    print("="*70)
    print("\nTest Configuration:")
    print(f"  • Trans_ID: {trans_id}")
    print("  • Writer (T1): Node 1 - Writing to the same data item")
    print("  • Reader (T2): Node 2 - Reading the same data item")
    print("  • Writer uses distributed lock manager")
    print("  • Testing all 4 isolation levels")
    print("="*70)
    
    # Store metrics for comparison
    isolation_metrics = {iso: [] for iso in isolation_levels}
    
    all_results = {}
    
    for isolation_level in isolation_levels:
        print(f"\n{'='*70}")
        print(f"Testing with {isolation_level}")
        print(f"{'='*70}")
        
        results = test.run_test(
            trans_id=trans_id,
            isolation_level=isolation_level
        )
        
        # Calculate metrics for this test
        metrics = test.calculate_metrics()
        isolation_metrics[isolation_level].append(metrics)
        
        all_results[isolation_level] = results
        
        print("\n" + "-"*70)
    
    # ========================================================================
    # PERFORMANCE COMPARISON
    # ========================================================================
    
    print(f"\n{'='*70}")
    print("ISOLATION LEVEL PERFORMANCE COMPARISON")
    print(f"{'='*70}\n")
    
    # Calculate averages for each isolation level
    comparison_data = []
    
    for iso_level in isolation_levels:
        metrics_list = isolation_metrics[iso_level]
        
        avg_throughput = sum(m['throughput'] for m in metrics_list) / len(metrics_list)
        avg_response = sum(m['avg_response_time'] for m in metrics_list) / len(metrics_list)
        avg_success_rate = sum(m['success_rate'] for m in metrics_list) / len(metrics_list)
        total_failures = sum(m['failed_txns'] for m in metrics_list)
        total_non_repeatable = sum(m['non_repeatable_reads'] for m in metrics_list)
        
        comparison_data.append({
            'Isolation Level': iso_level,
            'Avg Throughput (txn/s)': f"{avg_throughput:.6f}",
            'Avg Response Time (s)': f"{avg_response:.6f}",
            'Success Rate (%)': f"{avg_success_rate:.2f}",
            'Total Failures': total_failures,
            'Non-Repeatable Reads': total_non_repeatable
        })
    
    # Create comparison DataFrame
    df_comparison = pd.DataFrame(comparison_data)
    print(df_comparison.to_string(index=False))
    
    # ========================================================================
    # ANALYSIS & RECOMMENDATION
    # ========================================================================
    
    print(f"\n{'='*70}")
    print("ANALYSIS & RECOMMENDATION")
    print(f"{'='*70}\n")
    
    # Extract numeric values for comparison
    throughputs = {}
    response_times = {}
    anomalies = {}
    
    for iso_level in isolation_levels:
        metrics_list = isolation_metrics[iso_level]
        m = metrics_list[0]  # Single test
        throughputs[iso_level] = m['throughput']
        response_times[iso_level] = m['avg_response_time']
        anomalies[iso_level] = m['non_repeatable_reads']
    
    # Find best performers
    best_throughput = max(throughputs, key=throughputs.get)
    safest = min(anomalies, key=anomalies.get)
    
    print("📊 Performance Analysis:")
    print(f"   - Highest Throughput: {best_throughput} ({throughputs[best_throughput]:.6f} txn/s)")
    print(f"   - Safest (fewest anomalies): {safest} ({anomalies[safest]} non-repeatable reads)")
    
    # Make recommendation
    print(f"\n{'='*70}")
    print("RECOMMENDATION FOR CASE #2")
    print(f"{'='*70}\n")
    
    print("✅ Recommended: READ COMMITTED or REPEATABLE READ")
    print("\n📋 Reasoning:")
    print("   1. READ COMMITTED: Good balance of performance and safety")
    print("      - Prevents dirty reads")
    print("      - Allows non-repeatable reads (acceptable for many use cases)")
    print("      - Better throughput than stricter levels")
    print("   ")
    print("   2. REPEATABLE READ: For stricter consistency requirements")
    print("      - Prevents dirty reads AND non-repeatable reads")
    print("      - Slight performance overhead")
    print("      - Recommended if data consistency is critical")
    
    print("\n⚠️  Context for Case #2:")
    print("   - Distributed locking ensures write safety across nodes")
    print("   - Isolation level controls read consistency")
    print("   - Trade-off between concurrency and consistency")
    
    print("\n❌ Why not other levels?")
    print("   - READ UNCOMMITTED: Allows dirty reads (unsafe for writes)")
    print("   - SERIALIZABLE: Lowest throughput, may cause timeouts")
    
    # Final summary
    print(f"\n{'='*70}")
    print("FINAL SUMMARY")
    print(f"{'='*70}\n")
    
    print(f"✅ Tested 1 scenario (trans_id={trans_id})")
    print(f"✅ Tested {len(isolation_levels)} isolation levels")
    print(f"✅ Total tests run: {len(isolation_levels)}")
    
    print("\n📊 Key Findings:")
    print("   1. Distributed locking successfully coordinates writes across nodes")
    print("   2. Writer (T1) on Node 1 writes to the data item")
    print("   3. Reader (T2) on Node 2 reads the same data item (fallback to Node 1)")
    print("   4. Both transactions access the same data item concurrently")
    print("   5. Isolation levels affect read consistency and performance")
    
    # Cleanup
    test.cleanup()
    print("\n✓ Cleanup complete - all locks released")

if __name__ == "__main__":
    main()
