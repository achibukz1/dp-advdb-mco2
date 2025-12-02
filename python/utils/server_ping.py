"""
Server Ping Utility - Check database node connectivity
"""

import threading
import time
from datetime import datetime
import sys
import os

# Add parent directory to path for imports (fixes Streamlit Cloud deployment)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from python.db.db_config import get_db_connection


class NodePinger:
    """Background thread that pings database nodes every 5 seconds"""

    def __init__(self, interval=5):
        """
        Initialize the node pinger

        Args:
            interval: Time in seconds between pings (default: 5)
        """
        self.interval = interval
        self.running = False
        self.thread = None
        self.node_status = {1: False, 2: False, 3: False}
        self._last_check_time = 0
        self._check_cache_duration = 2  # Cache status checks for 2 seconds

    def check_node(self, node):
        """
        Check if a specific node is online

        Args:
            node: Node number (1, 2, or 3)

        Returns:
            bool: True if node is online, False otherwise
        """
        try:
            conn = get_db_connection(node)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            return True
        except Exception:
            return False

    def ping_all_nodes(self):
        """Check all nodes and print status"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        previous_status = self.node_status.copy()

        # Check each node
        for node in [1, 2, 3]:
            self.node_status[node] = self.check_node(node)

        # Detect nodes that came back online (no automatic recovery)
        recovered_nodes = []
        for node in [1, 2, 3]:
            if not previous_status.get(node, False) and self.node_status[node]:
                recovered_nodes.append(node)
                print(f"Node {node} came back online")

        # Build status message
        online_nodes = [node for node, status in self.node_status.items() if status]
        offline_nodes = [node for node, status in self.node_status.items() if not status]

        print(f"\n[{timestamp}] Node Status Check:")
        if online_nodes:
            print(f"  Online: Node {', Node '.join(map(str, online_nodes))}")
        if offline_nodes:
            print(f"  Offline: Node {', Node '.join(map(str, offline_nodes))}")
        
        # No automatic recovery notifications

        return self.node_status

    def _ping_loop(self):
        """Background loop that pings nodes at regular intervals"""
        while self.running:
            try:
                self.ping_all_nodes()
            except Exception as e:
                print(f"Error during node ping: {e}")

            # Sleep for the interval
            time.sleep(self.interval)

    def start(self):
        """Start the background ping thread"""
        if self.running:
            return

        self.running = True
        self.thread = threading.Thread(target=self._ping_loop, daemon=True)
        self.thread.start()
        print(f"Node pinger started (checking every {self.interval} seconds)")

    def stop(self):
        """Stop the background ping thread"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=self.interval + 1)
        print("Node pinger stopped")

    def get_status(self, force_refresh=False):
        """
        Get current node status with optional caching

        Args:
            force_refresh: Force a fresh check even if cache is valid

        Returns:
            dict: Node status dictionary {node_id: is_online}
        """
        current_time = time.time()

        # Use cached status if checked recently (unless force refresh)
        if not force_refresh and (current_time - self._last_check_time) < self._check_cache_duration:
            return self.node_status.copy()

        # Refresh status
        self._last_check_time = current_time
        return self.node_status.copy()

