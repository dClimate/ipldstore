"""
IPLD stores for zarr
"""

from .ipldstore import IPLDStore
from .contentstore import ContentAddressableStore, MappingCAStore, IPFSStore, FBLStore

def get_ipfs_mapper(host : str = "http://127.0.0.1:5001") -> IPLDStore:
    """
    Get an IPLDStore for IPFS running on the given host.
    """
    return IPLDStore(IPFSStore(host))

def get_fbl_mapper(host : str = "http://127.0.0.1:5001") -> IPLDStore:
    """
    Get an IPLDStore for IPFS running on the given host.
    """
    return IPLDStore(FBLStore(host))
