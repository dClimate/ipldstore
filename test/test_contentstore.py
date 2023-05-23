from ipfs_zarr_store.ipfs_content_store import IPFSContentStore

import pytest

test_values = [b"hallo", "hallo", {"a": 1}, [1, 2, 3], 1, 1.34, True, False, None]

@pytest.mark.parametrize("value", test_values)
def test_store_and_retrieve(value):
    s = IPFSContentStore(host="http://localhost:5001")
    cid = s.put(value)
    assert s.get(cid) == value
