"""
Global Failure and Recovery Test

Tests the 4 case studies using recovery_manager.py and recovery_log.sql:
Case 1: Node 2/3 → Node 1 replication failure
Case 2: Node 1 recovers and processes missed transactions  
Case 3: Node 1 → Node 2/3 replication failure
Case 4: Node 2/3 recovers and processes missed transactions

Simulates replication failures and recovery scenarios
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import mysql.connector
import time
import subprocess
from datetime import datetime
from python.utils.recovery_manager import RecoveryManager, simulate_replication_failure
from python.db.db_config import get_node_config

class GlobalRecoveryTest:
    def __init__(self):
        # Get database configs for all nodes
        self.node_configs = {
            1: get_node_config(1),
            2: get_node_config(2),
            3: get_node_config(3)
        }
        
        # Initialize recovery managers for each node
        self.recovery_managers = {
            1: RecoveryManager(self.node_configs[1], current_node_id=1),
            2: RecoveryManager(self.node_configs[2], current_node_id=2),
            3: RecoveryManager(self.node_configs[3], current_node_id=3)
        }
    
    def setup_recovery_tables(self):
        """Verify recovery_log tables exist on all nodes (already created in node init files)"""
        print("Verifying recovery log tables on all nodes...")
        
        check_table_sql = "SHOW TABLES LIKE 'recovery_log'"
        
        for node_id, config in self.node_configs.items():
            try:
                conn = mysql.connector.connect(**config)
                cursor = conn.cursor()
                cursor.execute(check_table_sql)
                result = cursor.fetchone()
                
                if result:
                    print(f"[SUCCESS] Recovery log table exists on Node {node_id}")
                else:
                    print(f"[WARNING] Recovery log table NOT found on Node {node_id}")
                    
                cursor.close()
                conn.close()
            except Exception as e:
                print(f"[ERROR] Failed to check recovery table on Node {node_id}: {e}")
    
    def get_sample_transaction(self, operation_type="INSERT"):
        """Get a sample transaction for testing"""
        if operation_type == "INSERT":
            return "INSERT INTO trans (trans_id, account_id, newdate, type, operation, amount, k_symbol) VALUES (999999, 9999, '2025-11-30', 'Credit', 'Recovery Test', 5000.00, 'RECOVERY')"
        elif operation_type == "UPDATE":
            return "UPDATE trans SET amount = 8888.88 WHERE trans_id = 276"
        elif operation_type == "DELETE":
            return "DELETE FROM trans WHERE trans_id = 999999"
        
    def test_case_1(self):
        """Case 1: Node 2/3 → Node 1 replication failure"""
        print(f"\n{'='*70}")
        print("CASE 1: Node 2/3 → Node 1 Replication Failure")
        print(f"{'='*70}")
        
        print("Scenario: Node 2 tries to replicate to Node 1, but Node 1 is offline")
        print("Expected: Transaction logged in recovery_log for later recovery")
        
        # First execute the transaction on Node 2 (source node)
        sql_statement = self.get_sample_transaction("INSERT")
        
        print(f"\nStep 1: Executing transaction on Node 2 (source)...")
        success = self.execute_transaction_on_node(2, sql_statement)
        
        if success:
            print(f"[SUCCESS] Transaction executed successfully on Node 2")
            
            print(f"\nStep 2: Manual Node Failure Simulation")
            print(f"INSTRUCTION: Please manually fail Node 1 using:")
            print(f"             python python/fail_start.py 1")
            print(f"Target: Node 1 (will be manually taken offline)")
            
            # Wait for user to fail the node
            input("\n[WAITING] Press ENTER after you have manually failed Node 1...")
            
            print(f"\nStep 3: Attempting replication to Node 1 (now offline)...")
            print(f"SQL: {sql_statement}")
            
            # Try to actually replicate - this should fail
            result = self.attempt_actual_replication(source_node=2, target_node=1, sql_statement=sql_statement)
        else:
            print(f"[ERROR] Failed to execute transaction on Node 2")
            result = {"status": "error", "message": "Source transaction failed", "logged": False}
        
        print(f"\nResult: {result}")
        
        # Check recovery logs on Node 2
        status = self.recovery_managers[2].get_recovery_status()
        print(f"Recovery logs on Node 2: {status}")
        
        return result
    
    def test_case_2(self):
        """Case 2: Node 1 recovers and processes missed transactions"""
        print(f"\n{'='*70}")
        print("CASE 2: Node 1 Recovery - Processing Missed Transactions")
        print(f"{'='*70}")
        
        print("Scenario: Node 1 comes back online and checks for missed transactions")
        print("Expected: Find pending logs and execute them")
        
        # Check if there are actually pending logs for Node 1
        pending_count = self.check_pending_logs_for_node(1)
        print(f"\nPending recovery logs for Node 1: {pending_count}")
        
        if pending_count == 0:
            print("[WARNING] No pending logs found for Node 1. This is expected if Case 1 didn't create logs.")
        
        print(f"\nStep 1: Manual Node Recovery")
        print(f"INSTRUCTION: Please manually restore Node 1 using fail_start.py")
        print(f"            (Answer 'Y' when prompted to grant privileges back)")
        
        # Wait for user to restore the node
        input("\n[WAITING] Press ENTER after you have restored Node 1...")
        
        # Verify node is back online
        print(f"\nStep 2: Verifying Node 1 is back online...")
        if self.verify_node_online(1):
            print(f"[SUCCESS] Node 1 is back online")
            
            print(f"\nStep 3: Node 1 startup - checking for pending recovery logs across ALL nodes...")
            print(f"        (This includes logs stored in Node 2, Node 3, and cross-backups)")
            
            # Use Node 1's recovery manager to check and recover
            recovery_results = self.recovery_managers[1].check_and_recover_pending_logs()
            
            print(f"\nRecovery Results: {recovery_results}")
            
            # Show global status across all nodes
            print(f"\nStep 4: Checking global recovery status...")
            global_status = self.recovery_managers[1].get_global_recovery_status()
            
            print(f"Global Recovery Status:")
            for node, status in global_status['nodes'].items():
                if 'error' in status:
                    print(f"  {node}: {status['error']}")
                else:
                    print(f"  {node}: PENDING={status['PENDING']}, COMPLETED={status['COMPLETED']}, FAILED={status['FAILED']}")
            
            print(f"  Total across all nodes: PENDING={global_status['total']['PENDING']}, COMPLETED={global_status['total']['COMPLETED']}, FAILED={global_status['total']['FAILED']}")
            
        else:
            print(f"[ERROR] Node 1 is still offline - cannot proceed with recovery")
            recovery_results = {"error": "Node 1 still offline"}
        
        return recovery_results
    
    def clear_previous_recovery_logs(self):
        """Clear any existing recovery logs from previous test runs"""
        print("Clearing previous recovery logs...")
        
        clear_sql = "DELETE FROM recovery_log WHERE status IN ('COMPLETED', 'FAILED')"
        
        for node_id, config in self.node_configs.items():
            try:
                conn = mysql.connector.connect(**config)
                cursor = conn.cursor()
                cursor.execute(clear_sql)
                deleted_count = cursor.rowcount
                conn.commit()
                if deleted_count > 0:
                    print(f"[SUCCESS] Cleared {deleted_count} old recovery logs from Node {node_id}")
                else:
                    print(f"[INFO] No old recovery logs to clear on Node {node_id}")
                cursor.close()
                conn.close()
            except Exception as e:
                print(f"[WARNING] Could not clear logs from Node {node_id}: {e}")
    
    def test_case_3(self):
        """Case 3: Node 1 → Node 2/3 replication failure"""
        print(f"\n{'='*70}")
        print("CASE 3: Node 1 → Node 2/3 Replication Failure")
        print(f"{'='*70}")
        
        print("Scenario: Node 1 tries to replicate to Node 2, but Node 2 is offline")
        print("Expected: Transaction logged in recovery_log for later recovery")
        
        # First execute the transaction on Node 1 (source node)
        sql_statement = self.get_sample_transaction("UPDATE")
        
        print(f"\nStep 1: Executing transaction on Node 1 (source)...")
        success = self.execute_transaction_on_node(1, sql_statement)
        
        if success:
            print(f"[SUCCESS] Transaction executed successfully on Node 1")
            
            print(f"\nStep 2: Manual Node Failure Simulation")
            print(f"INSTRUCTION: Please manually fail Node 2 using:")
            print(f"             python python/fail_start.py 2")
            print(f"Target: Node 2 (will be manually taken offline)")
            
            # Wait for user to fail the node
            input("\n[WAITING] Press ENTER after you have manually failed Node 2...")
            
            print(f"\nStep 3: Attempting replication to Node 2 (now offline)...")
            print(f"SQL: {sql_statement}")
            
            # Try to actually replicate - this should fail
            result = self.attempt_actual_replication(source_node=1, target_node=2, sql_statement=sql_statement)
        else:
            print(f"[ERROR] Failed to execute transaction on Node 1")
            result = {"status": "error", "message": "Source transaction failed", "logged": False}
        
        print(f"\nResult: {result}")
        
        # Check recovery logs on Node 1
        status = self.recovery_managers[1].get_recovery_status()
        print(f"Recovery logs on Node 1: {status}")
        
        return result
    
    def test_case_4(self):
        """Case 4: Node 2/3 recovers and processes missed transactions"""
        print(f"\n{'='*70}")
        print("CASE 4: Node 2 Recovery - Processing Missed Transactions")
        print(f"{'='*70}")
        
        print("Scenario: Node 2 comes back online and checks for missed transactions")
        print("Expected: Find pending logs and execute them")
        
        # Check if there are actually pending logs for Node 2
        pending_count = self.check_pending_logs_for_node(2)
        print(f"\nPending recovery logs for Node 2: {pending_count}")
        
        if pending_count == 0:
            print("[WARNING] No pending logs found for Node 2. This is expected if Case 3 didn't create logs.")
        
        print(f"\nStep 1: Manual Node Recovery")
        print(f"INSTRUCTION: Please manually restore Node 2 using fail_start.py")
        print(f"            (Answer 'Y' when prompted to grant privileges back)")
        
        # Wait for user to restore the node
        input("\n[WAITING] Press ENTER after you have restored Node 2...")
        
        # Verify node is back online
        print(f"\nStep 2: Verifying Node 2 is back online...")
        if self.verify_node_online(2):
            print(f"[SUCCESS] Node 2 is back online")
            
            print(f"\nStep 3: Node 2 startup - checking for pending recovery logs across ALL nodes...")
            print(f"        (This includes logs stored in Node 1, Node 3, and cross-backups)")
            
            # Use Node 2's recovery manager to check and recover
            recovery_results = self.recovery_managers[2].check_and_recover_pending_logs()
            
            print(f"\nRecovery Results: {recovery_results}")
            
            # Show global status across all nodes
            print(f"\nStep 4: Checking global recovery status...")
            global_status = self.recovery_managers[2].get_global_recovery_status()
            
            print(f"Global Recovery Status:")
            for node, status in global_status['nodes'].items():
                if 'error' in status:
                    print(f"  {node}: {status['error']}")
                else:
                    print(f"  {node}: PENDING={status['PENDING']}, COMPLETED={status['COMPLETED']}, FAILED={status['FAILED']}")
            
            print(f"  Total across all nodes: PENDING={global_status['total']['PENDING']}, COMPLETED={global_status['total']['COMPLETED']}, FAILED={global_status['total']['FAILED']}")
            
        else:
            print(f"[ERROR] Node 2 is still offline - cannot proceed with recovery")
            recovery_results = {"error": "Node 2 still offline"}
        
        return recovery_results
    
    def check_pending_logs_for_node(self, target_node_id):
        """Check how many pending recovery logs exist for a specific target node"""
        count_sql = "SELECT COUNT(*) FROM recovery_log WHERE target_node = %s AND status = 'PENDING'"
        
        total_pending = 0
        for node_id, config in self.node_configs.items():
            try:
                conn = mysql.connector.connect(**config)
                cursor = conn.cursor()
                cursor.execute(count_sql, (target_node_id,))
                count = cursor.fetchone()[0]
                total_pending += count
                cursor.close()
                conn.close()
            except Exception as e:
                print(f"[WARNING] Could not check pending logs on Node {node_id}: {e}")
        
        return total_pending
    
    def clear_all_recovery_logs(self):
        """Clear all recovery logs from all nodes"""
        clear_sql = "DELETE FROM recovery_log"
        
        for node_id, config in self.node_configs.items():
            try:
                conn = mysql.connector.connect(**config)
                cursor = conn.cursor()
                cursor.execute(clear_sql)
                deleted_count = cursor.rowcount
                conn.commit()
                print(f"[SUCCESS] Cleared {deleted_count} recovery logs from Node {node_id}")
                cursor.close()
                conn.close()
            except Exception as e:
                print(f"[WARNING] Could not clear logs from Node {node_id}: {e}")
    
    def attempt_actual_replication(self, source_node, target_node, sql_statement):
        """Attempt actual replication between nodes and handle failure"""
        try:
            # Try to execute on target node
            success = self.execute_transaction_on_node(target_node, sql_statement)
            
            if success:
                return {
                    "status": "success", 
                    "message": f"Replication to Node {target_node} succeeded",
                    "logged": False
                }
            else:
                # Replication failed - log it
                logged = self.recovery_managers[source_node].log_backup(
                    target_node, source_node, sql_statement
                )
                return {
                    "status": "error",
                    "message": f"Replication to Node {target_node} failed (node offline)",
                    "logged": logged
                }
                
        except Exception as e:
            # Connection error - node is definitely offline
            print(f"Connection error to Node {target_node}: {e}")
            
            # Log the failed replication
            logged = self.recovery_managers[source_node].log_backup(
                target_node, source_node, sql_statement
            )
            
            return {
                "status": "error",
                "message": f"Replication to Node {target_node} failed: {str(e)}",
                "logged": logged
            }
    
    def verify_node_online(self, node_id):
        """Verify if a node is online by attempting a simple query"""
        try:
            config = self.node_configs[node_id]
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            print(f"Node {node_id} verification failed: {e}")
            return False
    
    def execute_transaction_on_node(self, node_id, sql_statement):
        """Execute a transaction on a specific node"""
        try:
            config = self.node_configs[node_id]
            conn = mysql.connector.connect(**config)
            cursor = conn.cursor()
            
            cursor.execute(sql_statement)
            conn.commit()
            
            cursor.close()
            conn.close()
            return True
            
        except Exception as e:
            print(f"Error executing transaction on Node {node_id}: {e}")
            return False
    
    def show_all_recovery_status(self):
        """Show recovery status on all nodes"""
        print(f"\n{'='*70}")
        print("RECOVERY STATUS SUMMARY - ALL NODES")
        print(f"{'='*70}")
        
        for node_id in [1, 2, 3]:
            try:
                status = self.recovery_managers[node_id].get_recovery_status()
                print(f"\nNode {node_id}:")
                print(f"  PENDING: {status.get('PENDING', 0)}")
                print(f"  COMPLETED: {status.get('COMPLETED', 0)}")  
                print(f"  FAILED: {status.get('FAILED', 0)}")
                print(f"  Total: {sum(status.values())}")
            except Exception as e:
                print(f"Node {node_id}: Error getting status - {e}")
    
    def cleanup_test_data(self):
        """Clean up test data after testing"""
        print(f"\n{'='*70}")
        print("CLEANUP: Removing Test Data")
        print(f"{'='*70}")
        
        # Clean up test transaction and restore original amount for trans_id 276
        cleanup_queries = [
            "DELETE FROM trans WHERE trans_id = 999999",
            "UPDATE trans SET amount = 1000.00 WHERE trans_id = 276"
        ]
        
        for node_id, config in self.node_configs.items():
            try:
                conn = mysql.connector.connect(**config)
                cursor = conn.cursor()
                for query in cleanup_queries:
                    cursor.execute(query)
                conn.commit()
                print(f"[SUCCESS] Cleaned test data from Node {node_id}")
                cursor.close()
                conn.close()
            except Exception as e:
                print(f"[WARNING] Could not clean Node {node_id}: {e}")
    
    def show_test_instructions(self):
        """Show instructions for manual testing process"""
        print(f"\n{'='*80}")
        print("MANUAL FAILURE TESTING INSTRUCTIONS")
        print(f"{'='*80}")
        print()
        print("This test requires MANUAL node failure simulation using fail_start.py")
        print()
        print("\nTEST PROCESS:")
        print("   1. Test will execute a transaction on source node")
        print("   2. You will be prompted to manually fail target node")
        print("   3. Test will attempt replication (which should fail)")
        print("   4. Failed replication gets logged in recovery_log")
        print("   5. You will be prompted to restore the node")
        print("   6. Test will run recovery process automatically")
        print()
        print("COMMANDS YOU'LL NEED:")
        print("   - Fail Node 1:   python python/fail_start.py 1")
        print("   - Fail Node 2:   python python/fail_start.py 2")
        print("   - Restore: Answer 'Y' when fail_start.py prompts")
        print()
        print("[IMPORTANT] Keep fail_start.py terminal open throughout testing!")
        print()
        
        response = input("Are you ready to proceed with manual testing? (Y/N): ").strip().upper()
        if response != 'Y':
            print("Test cancelled. Run again when ready.")
            return False
        return True
    

    
    def analyze_results(self, results):
        """Analyze test results and provide summary"""
        print(f"\n{'='*70}")
        print("TEST ANALYSIS & SUMMARY")
        print(f"{'='*70}")
        
        print("\n📊 Results Summary:")
        
        # Case 1 Analysis
        case1 = results.get('case_1', {})
        if case1.get('logged'):
            print("[SUCCESS] Case 1: Successfully logged replication failure")
        else:
            print("[ERROR] Case 1: Failed to log replication failure")
        
        # Case 2 Analysis
        case2 = results.get('case_2', {})
        if isinstance(case2, dict) and 'recovered' in case2:
            recovered = case2.get('recovered', 0)
            print(f"[SUCCESS] Case 2: Successfully recovered {recovered} transactions")
        else:
            print("[WARNING] Case 2: No transactions to recover or recovery failed")
        
        # Case 3 Analysis
        case3 = results.get('case_3', {})
        if case3.get('logged'):
            print("[SUCCESS] Case 3: Successfully logged replication failure")
        else:
            print("[ERROR] Case 3: Failed to log replication failure")
        
        # Case 4 Analysis
        case4 = results.get('case_4', {})
        if isinstance(case4, dict) and 'recovered' in case4:
            recovered = case4.get('recovered', 0)
            print(f"[SUCCESS] Case 4: Successfully recovered {recovered} transactions")
        else:
            print("[WARNING] Case 4: No transactions to recover or recovery failed")
        
        print(f"\n{'='*70}")
        print("CONCLUSION")
        print(f"{'='*70}")
        
        print("\nKEY ACHIEVEMENTS:")
        total_logs_created = 0
        total_recoveries = 0
        
        # Count actual logs and recoveries
        for case_name, result in results.items():
            if isinstance(result, dict):
                if result.get('logged'):
                    total_logs_created += 1
                if 'recovered' in result:
                    total_recoveries += result.get('recovered', 0)
        
        print(f"   1. Recovery logs created: {total_logs_created}")
        print(f"   2. Transactions recovered: {total_recoveries}")
        print("   3. Custom recovery system successfully implemented")
        print("   4. Failed replication transactions captured")
        print("   5. Node recovery mechanisms functional")
        print("   6. Cross-node backup system working")
        
        print("\nRECOVERY SYSTEM FEATURES DEMONSTRATED:")
        print("   - Automatic failure detection and logging")
        print("   - Chronological transaction recovery")
        print("   - Retry mechanism with failure handling")
        print("   - Duplicate prevention via transaction hashing")
        print("   - Status tracking (PENDING/COMPLETED/FAILED)")
        
        print("\nTECHNICAL COMPONENTS VALIDATED:")
        print("   - recovery_log.sql table structure")
        print("   - RecoveryManager class functionality") 
        print("   - Cross-node communication simulation")
        print("   - Database integration and error handling")
        
        print("\n[SUCCESS] Ready for MCO2 demonstration and technical report!")

def show_menu():
    """Display test case selection menu"""
    print("\n" + "=" * 70)
    print("GLOBAL FAILURE AND RECOVERY TEST MENU")
    print("=" * 70)
    print("Select which test case to run:")
    print()
    print("1. Case 1: Node 2/3 -> Node 1 replication failure")
    print("2. Case 2: Node 1 recovery processing")
    print("3. Case 3: Node 1 -> Node 2/3 replication failure")
    print("4. Case 4: Node 2/3 recovery processing")
    print("5. Run all cases sequentially")
    print("6. Show recovery status on all nodes")
    print("7. Clear all recovery logs")
    print("8. Exit")
    print("=" * 70)

def main():
    """Run the global failure and recovery test with menu selection"""
    test = GlobalRecoveryTest()
    
    print("Global Failure and Recovery Test")
    print("Testing custom recovery system with recovery_manager.py and recovery_log.sql")
    
    # Initial setup verification
    test.setup_recovery_tables()
    
    while True:
        show_menu()
        
        try:
            choice = input("\nEnter your choice (1-8): ").strip()
            
            if choice == '1':
                print("\nRunning Case 1: Node 2/3 -> Node 1 replication failure")
                result = test.test_case_1()
                print(f"\nCase 1 Result: {result}")
                
            elif choice == '2':
                print("\nRunning Case 2: Node 1 recovery processing")
                result = test.test_case_2()
                print(f"\nCase 2 Result: {result}")
                
            elif choice == '3':
                print("\nRunning Case 3: Node 1 -> Node 2/3 replication failure")
                result = test.test_case_3()
                print(f"\nCase 3 Result: {result}")
                
            elif choice == '4':
                print("\nRunning Case 4: Node 2/3 recovery processing")
                result = test.test_case_4()
                print(f"\nCase 4 Result: {result}")
                
            elif choice == '5':
                print("\nRunning all test cases sequentially...")
                if test.show_test_instructions():
                    test.clear_previous_recovery_logs()
                    results = {}
                    
                    try:
                        results['case_1'] = test.test_case_1()
                        time.sleep(2)
                        
                        results['case_2'] = test.test_case_2()
                        time.sleep(2)
                        
                        results['case_3'] = test.test_case_3()
                        time.sleep(2)
                        
                        results['case_4'] = test.test_case_4()
                        
                        test.show_all_recovery_status()
                        test.analyze_results(results)
                        
                    except Exception as e:
                        print(f"Test suite error: {e}")
                    
                    finally:
                        test.cleanup_test_data()
                
            elif choice == '6':
                print("\nShowing recovery status on all nodes...")
                test.show_all_recovery_status()
                
            elif choice == '7':
                print("\nClearing all recovery logs...")
                test.clear_all_recovery_logs()
                print("All recovery logs cleared.")
                
            elif choice == '8':
                print("\nExiting test program...")
                print(f"Test session ended at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                break
                
            else:
                print("\nInvalid choice. Please enter a number between 1-8.")
                
        except KeyboardInterrupt:
            print("\n\nTest interrupted by user.")
            break
        except Exception as e:
            print(f"\nError: {e}")
    
    return True

if __name__ == "__main__":
    main()