from ipfs_zarr_store import IPFSZarrStore

def test_basic_mapping_properties():
    s = IPFSZarrStore(host="http://localhost:5001")
    s["a"] = b"b"
    assert s["a"] == b"b"
    assert len(s) == 1

def test_iterate_store_hierarchy():
    s = IPFSZarrStore(host="http://localhost:5001")
    s[".zgroup"] = b'{"test": 123}'
    s["a/b"] = b"c"
    s["d"] = b"e"
    assert list(sorted(s)) == [".zgroup", "a/b", "d"]
