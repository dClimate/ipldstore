from py_hamt import Store

# Provide a Store interface for HAMT
# class IPFSStore(Store):
#     """Use IPFS as a backing store for a HAMT. The IDs returned from save and used by load are IPFS CIDs."""
#     def __init__(self):


#     def save(self, node: bytes) -> bytes:
#         return super().save(node)

#     def load(self, id: bytes) -> bytes:
#         return super().load(id)
