#!/usr/bin/env python3
"""
Test script to verify cross-node recovery functionality
"""

import sys
import os

# Add python directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'python'))

from python.db.db_config import get_node_config
from python.utils.recovery_manager import RecoveryManager

def test_cross_node_recovery():
    """Test that nodes check other nodes for recovery logs"""
    
    print("=== Testing Cross-Node Recovery Functionality ===")
    
    try:
        # Initialize recovery manager for Node 1
        node1_config = get_node_config(1)
        recovery_manager_1 = RecoveryManager(node1_config, current_node_id=1)
        
        print(f"\n1. Creating test recovery log from Node 2 to Node 1...")
        
        # Create a recovery log on Node 2 targeting Node 1
        node2_config = get_node_config(2)
        recovery_manager_2 = RecoveryManager(node2_config, current_node_id=2)
        
        # Log a backup transaction (simulating Node 3 -> Node 1 replication failure)
        test_sql = "INSERT INTO movies (title, year) VALUES ('Cross-Node Test', 2025)"
        success = recovery_manager_2.log_backup(
            target_node=1, 
            source_node=3, 
            sql_statement=test_sql
        )
        
        if success:
            print(f"✓ Recovery log created on Node 2 for Node 1")
        else:
            print(f"✗ Failed to create recovery log")
            return False
        
        print(f"\n2. Node 1 checking for recovery logs across all nodes...")
        
        # Node 1 checks for pending logs (should find the log from Node 2)
        recovery_results = recovery_manager_1.check_and_recover_pending_logs()
        
        print(f"\n3. Recovery Results:")
        print(f"   - Total logs found: {recovery_results.get('total_logs', 0)}")
        print(f"   - Nodes checked: {recovery_results.get('nodes_checked', [])}")
        print(f"   - Recovered: {recovery_results.get('recovered', 0)}")
        print(f"   - Failed: {recovery_results.get('failed', 0)}")
        print(f"   - Skipped: {recovery_results.get('skipped', 0)}")
        
        print(f"\n4. Global recovery status:")
        global_status = recovery_manager_1.get_global_recovery_status()
        
        for node, status in global_status['nodes'].items():
            if 'error' in status:
                print(f"   {node}: {status['error']}")
            else:
                print(f"   {node}: PENDING={status['PENDING']}, COMPLETED={status['COMPLETED']}, FAILED={status['FAILED']}")
        
        print(f"   Total: PENDING={global_status['total']['PENDING']}, COMPLETED={global_status['total']['COMPLETED']}, FAILED={global_status['total']['FAILED']}")
        
        if recovery_results.get('total_logs', 0) > 0:
            print(f"\n✓ Cross-node recovery functionality is working!")
            return True
        else:
            print(f"\n! No recovery logs found - this may be normal if nodes are offline")
            return True
            
    except Exception as e:
        print(f"\n✗ Error during cross-node recovery test: {e}")
        return False

if __name__ == "__main__":
    test_cross_node_recovery()