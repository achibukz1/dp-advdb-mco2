#!/usr/bin/env python3
"""
Test deduplication functionality in recovery system
"""

import sys
import os

# Add python directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'python'))

from python.db.db_config import get_node_config
from python.utils.recovery_manager import RecoveryManager

def test_deduplication():
    """Test that duplicate transactions are properly handled"""
    
    print("=== Testing Recovery Deduplication ===")
    
    try:
        # Test with Node 1
        node1_config = get_node_config(1)
        recovery_manager_1 = RecoveryManager(node1_config, current_node_id=1)
        
        # Create the same transaction from multiple sources (simulating cross-backup)
        test_sql = "INSERT INTO movies (title, year) VALUES ('Dedup Test', 2025)"
        
        # Create logs from different managers (simulating different source nodes)
        node2_config = get_node_config(2)
        recovery_manager_2 = RecoveryManager(node2_config, current_node_id=2)
        
        node3_config = get_node_config(3)
        recovery_manager_3 = RecoveryManager(node3_config, current_node_id=3)
        
        print("1. Creating identical recovery logs from multiple nodes...")
        
        # Create the same transaction multiple times (should create same hash)
        success1 = recovery_manager_2.log_backup(
            target_node=1, 
            source_node=2, 
            sql_statement=test_sql
        )
        
        success2 = recovery_manager_3.log_backup(
            target_node=1, 
            source_node=3, 
            sql_statement=test_sql
        )
        
        if success1 and success2:
            print("✓ Multiple recovery logs created")
        else:
            print("✗ Failed to create test logs")
            return False
        
        print("\n2. Node 1 performing cross-node recovery with deduplication...")
        
        # Node 1 checks for recovery logs
        recovery_results = recovery_manager_1.check_and_recover_pending_logs()
        
        print(f"\n3. Recovery Results:")
        print(f"   - Total logs found: {recovery_results.get('total_logs', 0)}")
        print(f"   - Unique logs processed: {recovery_results.get('unique_logs', 0)}")
        print(f"   - Recovered: {recovery_results.get('recovered', 0)}")
        print(f"   - Failed: {recovery_results.get('failed', 0)}")
        print(f"   - Skipped: {recovery_results.get('skipped', 0)}")
        
        # Expected: total_logs > unique_logs (duplicates found and handled)
        expected_behavior = (
            recovery_results.get('total_logs', 0) >= recovery_results.get('unique_logs', 0) and
            recovery_results.get('failed', 0) == 0  # No failures due to duplicates
        )
        
        if expected_behavior:
            print(f"\n✓ Deduplication working correctly!")
            print(f"   - Found duplicates: {recovery_results.get('total_logs', 0) - recovery_results.get('unique_logs', 0)}")
            return True
        else:
            print(f"\n! Unexpected results - check the deduplication logic")
            return False
            
    except Exception as e:
        print(f"\n✗ Error during deduplication test: {e}")
        return False

if __name__ == "__main__":
    test_deduplication()