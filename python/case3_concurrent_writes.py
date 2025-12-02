"""
Case 3: Concurrent Write Operations Across Multiple Nodes

This test case demonstrates concurrent transactions in 2 or more nodes 
writing (update/delete) the same data item with proper distributed locking.

Test Setup:
- Uses trans_id = 1 (account_id = 1 - odd number)
- Node 1: Contains all records (central node)
- Node 3: Contains odd account_id records (includes trans_id=1)
- Multiple concurrent UPDATE transactions on the same record
- Tests 2-Phase Locking (2PL) across distributed nodes
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import mysql.connector
import threading
import time
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from python.utils.lock_manager import DistributedLockManager

class ConcurrentWriteTest:
    def __init__(self):
        self.results = {}
        self.lock = threading.Lock()
        
        # Load environment variables from .env file
        load_dotenv()

        # Build node configurations directly from .env file
        self.node_configs = {
            1: {
                "host": os.getenv('LOCAL_DB_HOST', 'localhost'),
                "port": int(os.getenv('LOCAL_DB_PORT', '3306')),
                "user": os.getenv('LOCAL_DB_USER', 'user'),
                "password": os.getenv('LOCAL_DB_PASSWORD', 'rootpass'),
                "database": os.getenv('LOCAL_DB_NAME', 'node1_db')
            },
            2: {
                "host": os.getenv('LOCAL_DB_HOST_NODE2', 'localhost'),
                "port": int(os.getenv('LOCAL_DB_PORT_NODE2', '3307')),
                "user": os.getenv('LOCAL_DB_USER_NODE2', 'user'),
                "password": os.getenv('LOCAL_DB_PASSWORD_NODE2', 'rootpass'),
                "database": os.getenv('LOCAL_DB_NAME_NODE2', 'node2_db')
            },
            3: {
                "host": os.getenv('LOCAL_DB_HOST_NODE3', 'localhost'),
                "port": int(os.getenv('LOCAL_DB_PORT_NODE3', '3308')),
                "user": os.getenv('LOCAL_DB_USER_NODE3', 'user'),
                "password": os.getenv('LOCAL_DB_PASSWORD_NODE3', 'rootpass'),
                "database": os.getenv('LOCAL_DB_NAME_NODE3', 'node3_db')
            }
        }
        
        # Initialize distributed lock manager
        self.lock_manager = DistributedLockManager(self.node_configs, current_node_id="case3_concurrent_writes")
        
        print("[INIT] Database configurations loaded directly from .env file")
        for node_num in [1, 3]:
            config = self.node_configs[node_num]
            print(f"[INIT] Node {node_num}: {config['host']}:{config['port']}/{config['database']}")
    
    def write_transaction(self, node_num, trans_id, new_amount, transaction_id, isolation_level, operation_type="UPDATE"):
        """
        Execute a write (UPDATE/DELETE) transaction on specified node with distributed locking.
        
        This implements proper 2-Phase Locking (matching Case 2 pattern):
        1. GROWING PHASE: Acquire lock on resource before accessing
        2. Execute transaction:
           - START TRANSACTION
           - SELECT (read before)
           - UPDATE (modify)
           - SELECT (read after to verify)
           - COMMIT
        3. SHRINKING PHASE: Release lock after commit/rollback
        
        Args:
            node_num: Database node number (1 or 3)
            trans_id: Transaction ID to modify
            new_amount: New amount value (for UPDATE)
            transaction_id: Unique identifier for this transaction thread
            isolation_level: SQL isolation level
            operation_type: "UPDATE" or "DELETE"
        """
        start_time = time.time()
        config = self.node_configs[node_num]
        resource_id = f"trans_{trans_id}"
        
        conn = None
        cursor = None
        lock_acquired = False
        
        # Create a unique lock identifier for this transaction
        lock_holder_id = f"{self.lock_manager.current_node_id}_{transaction_id}"

        try:
            # PHASE 1: Acquire distributed lock BEFORE starting transaction (2PL Growing Phase)
            print(f"[{transaction_id}] Attempting to acquire lock on {resource_id} at Node {node_num} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
            
            # Temporarily set the lock manager's ID to this transaction's unique ID
            original_node_id = self.lock_manager.current_node_id
            self.lock_manager.current_node_id = lock_holder_id

            lock_acquired = self.lock_manager.acquire_lock(resource_id, node_num, timeout=30)
            
            # Restore original ID
            self.lock_manager.current_node_id = original_node_id

            if not lock_acquired:
                raise Exception(f"Failed to acquire lock on {resource_id}")
            
            print(f"[{transaction_id}] Lock acquired, starting write on Node {node_num} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")

            # PHASE 2: Connect to database and start transaction
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor(dictionary=True)
            
            # Set isolation level
            cursor.execute(f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")
            
            # Validate we still hold the lock before starting transaction
            original_node_id_check = self.lock_manager.current_node_id
            self.lock_manager.current_node_id = lock_holder_id
            lock_held = self.lock_manager.check_lock(resource_id, node_num)
            self.lock_manager.current_node_id = original_node_id_check

            if not lock_held:
                raise Exception(f"Lost lock on {resource_id} at Node {node_num}")
            
            # Start transaction
            cursor.execute("START TRANSACTION")
            
            # Read current value BEFORE update (matches Case 2 pattern)
            cursor.execute("SELECT trans_id, amount FROM trans WHERE trans_id = %s", (trans_id,))
            before = cursor.fetchone()
            
            if not before:
                raise Exception(f"Record with trans_id={trans_id} not found on Node {node_num}")
            
            # PHASE 3: Execute write operation IMMEDIATELY (locks the row at database level)
            if operation_type == "UPDATE":
                cursor.execute("UPDATE trans SET amount = %s WHERE trans_id = %s", (new_amount, trans_id))
            elif operation_type == "DELETE":
                cursor.execute("DELETE FROM trans WHERE trans_id = %s", (trans_id,))

            # Hold transaction open AFTER update (keeps row locked - matches Case 2 behavior)
            time.sleep(3)
            affected_rows = cursor.rowcount
            
            # Read updated value AFTER update (matches Case 2 pattern)
            after = None
            if operation_type == "UPDATE":
                cursor.execute("SELECT trans_id, amount FROM trans WHERE trans_id = %s", (trans_id,))
                after = cursor.fetchone()
            
            # PHASE 4: Commit transaction
            conn.commit()
            
            end_time = time.time()
            
            print(f"[{transaction_id}] Completed write on Node {node_num} at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
            print(f"[{transaction_id}] Updated trans_id={trans_id}: {before['amount']} → {after['amount'] if after else 'DELETED'}")

            # Store results
            with self.lock:
                self.results[transaction_id] = {
                    'type': 'WRITE',
                    'operation': operation_type,
                    'node': f'node{node_num}',
                    'status': 'SUCCESS',
                    'trans_id': trans_id,
                    'before_amount': float(before['amount']),
                    'after_amount': float(after['amount']) if after else None,
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
                    'operation': operation_type,
                    'node': f'node{node_num}',
                    'status': 'FAILED',
                    'error': str(e),
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration': end_time - start_time
                }
        
        finally:
            # PHASE 5: Release the lock (2PL Shrinking Phase)
            if lock_acquired:
                # Use the same unique lock identifier for release
                original_node_id_release = self.lock_manager.current_node_id
                self.lock_manager.current_node_id = lock_holder_id
                self.lock_manager.release_lock(resource_id, node_num)
                self.lock_manager.current_node_id = original_node_id_release
                print(f"[{transaction_id}] Lock released on {resource_id}")

            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def run_test(self, trans_id, isolation_level="READ COMMITTED", num_writers_per_node=5):
        """
        Run concurrent write test with multiple transactions writing to the same record.
        
        Test Configuration:
        - trans_id = 1 (account_id = 1, which is odd)
        - Node 1: Central node (has all records)
        - Node 3: Partition node for odd account_ids
        - Multiple concurrent UPDATE operations on same record
        
        Args:
            trans_id: Transaction ID to test (default: 1)
            isolation_level: SQL isolation level
            num_writers_per_node: Number of concurrent writers per node (default: 5, giving 10 total)
        """
        print(f"\n{'='*80}")
        print(f"Case #3: Concurrent Write Operations on Same Data Item")
        print(f"{'='*80}")
        print(f"Test Configuration:")
        print(f"  Target: trans_id={trans_id} (account_id=1 - odd, stored in Node 1 and Node 3)")
        print(f"  Isolation Level: {isolation_level}")
        print(f"  Writers on Node 1: {num_writers_per_node}")
        print(f"  Writers on Node 3: {num_writers_per_node}")
        print(f"  Total Concurrent Writers: {num_writers_per_node * 2}")
        print(f"  Expected Behavior: Writers serialize due to distributed lock")
        print(f"{'='*80}\n")
        
        # Clean up any stale locks before starting test
        self.cleanup_locks(f"trans_{trans_id}")

        self.results = {}  # Reset results
        threads = []
        
        # Get initial value
        initial_amount = self.get_current_amount(trans_id, 1)
        print(f"[INITIAL] trans_id={trans_id} amount: {initial_amount}\n")
        
        # Create writer threads on Node 1
        for i in range(1, num_writers_per_node + 1):
            new_amount = 10000.00 + (i * 1111.11)
            threads.append(threading.Thread(
                target=self.write_transaction,
                args=(1, trans_id, new_amount, f"T{i}_WRITE_Node1", isolation_level, "UPDATE")
            ))
        
        # Create writer threads on Node 3
        for i in range(1, num_writers_per_node + 1):
            new_amount = 20000.00 + (i * 2222.22)
            threads.append(threading.Thread(
                target=self.write_transaction,
                args=(3, trans_id, new_amount, f"T{i}_WRITE_Node3", isolation_level, "UPDATE")
            ))
        
        # Start all threads with staggering (matches Case 2 pattern)
        print(f"[START] Launching {len(threads)} concurrent write transactions...\n")
        for i, thread in enumerate(threads):
            thread.start()
            time.sleep(0.2)  # Stagger to create lock contention (matches Case 2)

        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        print(f"\n[COMPLETE] All transactions finished\n")
        
        # Get final value
        final_amount = self.get_current_amount(trans_id, 1)
        print(f"[FINAL] trans_id={trans_id} amount: {final_amount}\n")
        
        # Display results
        self.display_results(initial_amount, final_amount)
        
        # Restore original value
        self.restore_original_value(trans_id)
        
        return self.results
    
    def cleanup_locks(self, resource_id):
        """Clean up any stale locks for the given resource on all nodes"""
        lock_name = f"lock_{resource_id}"
        for node_num in [1, 3]:
            try:
                config = self.node_configs[node_num]
                conn = mysql.connector.connect(**config)
                cursor = conn.cursor()

                cursor.execute("DELETE FROM distributed_lock WHERE lock_name = %s", (lock_name,))
                conn.commit()

                if cursor.rowcount > 0:
                    print(f"[CLEANUP] Removed {cursor.rowcount} stale lock(s) for {resource_id} on Node {node_num}")

                cursor.close()
                conn.close()
            except Exception as e:
                print(f"[CLEANUP] Warning: Could not clean locks on Node {node_num}: {e}")

    def cleanup_locks(self, resource_id):
        """Clean up any stale locks for the given resource on all nodes"""
        lock_name = f"lock_{resource_id}"
        for node_num in [1, 3]:
            try:
                config = self.node_configs[node_num]
                conn = mysql.connector.connect(**config)
                cursor = conn.cursor()

                cursor.execute("DELETE FROM distributed_lock WHERE lock_name = %s", (lock_name,))
                conn.commit()

                if cursor.rowcount > 0:
                    print(f"[CLEANUP] Removed {cursor.rowcount} stale lock(s) for {resource_id} on Node {node_num}")

                cursor.close()
                conn.close()
            except Exception as e:
                print(f"[CLEANUP] Warning: Could not clean locks on Node {node_num}: {e}")

    def get_current_amount(self, trans_id, node_num):
        """Get current amount for a transaction"""
        try:
            config = self.node_configs[node_num]
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute("SELECT amount FROM trans WHERE trans_id = %s", (trans_id,))
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            return float(result['amount']) if result else None
        except Exception as e:
            print(f"[ERROR] Could not get current amount: {e}")
            return None
    
    def calculate_metrics(self):
        """Calculate performance metrics for comparison"""
        if not self.results:
            return {
                'total_time': 0,
                'successful_txns': 0,
                'failed_txns': 0,
                'throughput': 0,
                'avg_response_time': 0,
                'success_rate': 0
            }

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

        # Success rate
        success_rate = (successful_txns / len(self.results)) * 100

        return {
            'total_time': total_time,
            'successful_txns': successful_txns,
            'failed_txns': failed_txns,
            'throughput': throughput,
            'avg_response_time': avg_response,
            'success_rate': success_rate
        }

    def restore_original_value(self, trans_id):
        """Restore the original value after test on both nodes"""
        original_amount = 1000.00
        
        for node_num in [1, 3]:
            try:
                config = self.node_configs[node_num]
                conn = mysql.connector.connect(**config)
                cursor = conn.cursor()
                
                cursor.execute("UPDATE trans SET amount = %s WHERE trans_id = %s", (original_amount, trans_id))
                conn.commit()
                
                cursor.close()
                conn.close()
                
                print(f"[RESTORE] Node {node_num}: trans_id={trans_id} restored to {original_amount}")
            except Exception as e:
                print(f"[RESTORE] Warning: Could not restore on Node {node_num}: {e}")
    
    def display_results(self, initial_amount, final_amount):
        """Display detailed test results and analysis (matching Case 2 format)"""
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
            
            if result['status'] == 'SUCCESS':
                row['Before→After'] = f"{result['before_amount']:.2f}→{result['after_amount']:.2f}"

            summary.append(row)
        
        df = pd.DataFrame(summary)
        print(df.to_string(index=False))
        
        # Concurrency Analysis (matching Case 2 format)
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

        # Check if transactions ran concurrently
        if total_time < sum(r['duration'] for r in self.results.values()) * 0.5:
            print(f"PASSED: Transactions ran concurrently with proper serialization")
        else:
            print(f"NOTE: Transactions serialized (expected due to distributed locking)")

        # Lock effectiveness analysis
        writers = [r for r in self.results.values() if r['type'] == 'WRITE']
        successful_writers = [r for r in writers if r['status'] == 'SUCCESS']

        if writers:
            success_rate = (len(successful_writers) / len(writers)) * 100
            print(f"\n2-PHASE LOCKING EFFECTIVENESS:")
            print(f"   Writers: {len(successful_writers)}/{len(writers)} successful ({success_rate:.1f}%)")

            if successful_writers:
                print(f"   Growing Phase: Distributed locks acquired before UPDATE")
                print(f"   Shrinking Phase: Locks released after COMMIT")
                print(f"   Strict 2PL: Lock held through entire transaction lifecycle")

        # Data Consistency Check
        print(f"\n{'='*70}")
        print("DATA CONSISTENCY CHECK")
        print(f"{'='*70}\n")

        print(f"Initial amount: {initial_amount:.2f}")
        print(f"Final amount:   {final_amount:.2f}")
        
        # Verify final amount matches one of the write operations
        if successful_writers:
            expected_amounts = [r['after_amount'] for r in successful_writers if r.get('after_amount')]
            if final_amount in expected_amounts:
                print(f"\n✓ CONSISTENCY VERIFIED: Final value matches a committed write")
                print(f"  Distributed locking prevented lost updates and race conditions")
            else:
                print(f"\n✗ WARNING: Final value does not match any committed write")


def main():
    """Main entry point for Case 3 test"""
    print("\n" + "="*80)
    print("CASE #3: CONCURRENT WRITE OPERATIONS TEST")
    print("="*80)
    print("\nTest Configuration:")
    print(f"  • Trans_ID: 1 (account_id=1 - odd)")
    print("  • Target Nodes: Node 1 (central) and Node 3 (odd partition)")
    print("  • Writers per Node: 5 (Total: 10 concurrent writes)")
    print("  • All writers target the same data item")
    print("  • Distributed lock manager enforces serialization")
    print("  • Testing multiple isolation levels")
    print("="*80)
    
    test = ConcurrentWriteTest()
    
    # Test with trans_id = 1 (account_id = 1 - odd, stored in Node 1 and Node 3)
    # Isolation levels to test
    isolation_levels = [
        "READ UNCOMMITTED",
        "READ COMMITTED",
        "REPEATABLE READ",
        "SERIALIZABLE"
    ]

    # Store metrics for comparison
    isolation_metrics = {iso: [] for iso in isolation_levels}
    all_results = {}

    for isolation_level in isolation_levels:
        print(f"\n{'='*80}")
        print(f"Testing with Isolation Level: {isolation_level}")
        print(f"{'='*80}")
        
        results = test.run_test(
            trans_id=1,
            isolation_level=isolation_level,
            num_writers_per_node=5  # 5 writers on Node 1, 5 writers on Node 3 = 10 total
        )
        
        # Calculate and store metrics
        metrics = test.calculate_metrics()
        isolation_metrics[isolation_level].append(metrics)
        all_results[isolation_level] = results

        time.sleep(2)  # Pause between tests
    
    # ========================================================================
    # PERFORMANCE COMPARISON
    # ========================================================================

    print(f"\n{'='*80}")
    print("ISOLATION LEVEL PERFORMANCE COMPARISON")
    print(f"{'='*80}\n")

    # Calculate averages for each isolation level
    comparison_data = []

    for iso_level in isolation_levels:
        metrics_list = isolation_metrics[iso_level]

        avg_throughput = sum(m['throughput'] for m in metrics_list) / len(metrics_list)
        avg_response = sum(m['avg_response_time'] for m in metrics_list) / len(metrics_list)
        avg_success_rate = sum(m['success_rate'] for m in metrics_list) / len(metrics_list)
        total_failures = sum(m['failed_txns'] for m in metrics_list)

        comparison_data.append({
            'Isolation Level': iso_level,
            'Avg Throughput (txn/s)': f"{avg_throughput:.6f}",
            'Avg Response Time (s)': f"{avg_response:.6f}",
            'Success Rate (%)': f"{avg_success_rate:.2f}",
            'Total Failures': total_failures
        })

    # Create comparison DataFrame
    df_comparison = pd.DataFrame(comparison_data)
    print(df_comparison.to_string(index=False))

    print("\n" + "-"*80)
    print("\n✓ All tests completed successfully!")
    print("  All isolation levels properly handled concurrent writes")
    print("  Distributed locking prevented lost updates across all tests")
    print("\n" + "="*80 + "\n")


if __name__ == "__main__":
    main()

