import hashlib
import bisect

class ConsistentHashRing:
    """
    Implements a consistent hash ring that supports virtual nodes and replication.
    """
    def __init__(self, nodes: list[str], replicas: int = 256): # Increased replicas for better distribution
        self.replicas = replicas
        self._ring = {}
        self._sorted_keys = []

        for node in nodes:
            self.add_node(node)
        
        print(f"ConsistentHashRing initialized with {len(nodes)} nodes and {self.replicas} replicas each.")

    def add_node(self, node: str):
        """Adds a physical node to the hash ring."""
        for i in range(self.replicas):
            key = f"{node}:{i}"
            h = self._hash(key)
            self._ring[h] = node
            bisect.insort(self._sorted_keys, h)

    def get_node(self, key: str) -> str:
        """DEPRECATED: Use get_nodes instead. Finds the primary node for a key."""
        nodes = self.get_nodes(key, replica_count=1)
        return nodes[0] if nodes else None


    def get_nodes(self, key: str, replica_count: int) -> list[str]:
        """
        Finds a list of unique nodes responsible for the given key.
        This is the core of our replication strategy.
        """
        if not self._ring or replica_count == 0:
            return []

        # Ensure we don't try to find more replicas than physical nodes exist.
        unique_nodes = set(self._ring.values())
        if replica_count > len(unique_nodes):
            replica_count = len(unique_nodes)

        h = self._hash(key)
        idx = bisect.bisect_left(self._sorted_keys, h)
        
        nodes = []
        # We walk the ring clockwise to find unique nodes for replication.
        while len(nodes) < replica_count:
            if idx == len(self._sorted_keys):
                idx = 0
            
            node_hash = self._sorted_keys[idx]
            node = self._ring[node_hash]
            
            # Add the node only if we haven't added it already.
            if node not in nodes:
                nodes.append(node)
            
            idx += 1
        
        return nodes

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16) & 0xFFFFFFFF