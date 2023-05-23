"""
IPLD store for zarr
"""

from .ipldstore import IPFSZarrStore
from .ipfs_content_store import IPFSContentStore


def get_ipfs_mapper(
    host: str = "http://127.0.0.1:5001",
    max_nodes_per_level: int = 10000,
    chunker: str = "size-262144",
    should_async_get: bool = True,
) -> IPFSZarrStore:
    """
    Get an IPLDStore for IPFS running on the given host.
    """
    return IPFSZarrStore(
        host,
        IPFSContentStore(host, chunker=chunker, max_nodes_per_level=max_nodes_per_level),
        should_async_get=should_async_get,
    )
