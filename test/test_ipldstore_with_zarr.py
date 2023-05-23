from ipfs_zarr_store import IPFSZarrStore
from ipfs_zarr_store.ipfs_content_store import IPFSContentStore

import zarr
import numpy as np
from multiformats import CID


def test_create_array():
    ipfs_store = IPFSContentStore("http://localhost:5001")
    store = IPFSZarrStore("http://localhost:5001", ipfs_store)
    z = zarr.create(store=store, overwrite=True, shape=5, dtype='i1', compressor=None)
    z[:] = np.arange(5, dtype="i1")
    assert CID.decode("Qma4sWyoTiJKYx1wTjkxWY71dYg1oWgE7SE9oiTGqGnyoR") in ipfs_store  # b"\x00\x01\x02\x03\x04"
