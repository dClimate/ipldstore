from copy import deepcopy
from threading import Lock
from collections.abc import MutableMapping
from multiformats import CID
from py_hamt import HAMT, Store
import requests
from msgspec import json


class IPFSStore(Store):
    """
    Copied from py-hamt's IPFS Store, with modifications for:
    + uses a different api for uploading blobs to allow for arbitrary size blobs
    + allows custom CID codecs per store
    + uses blake3 has the CID hash type by default
    """

    def __init__(
        self,
        timeout_seconds=30,
        gateway_uri_stem="http://127.0.0.1:8080",
        rpc_uri_stem="http://127.0.0.1:5001",
        cid_codec="dag-cbor",
    ):
        self.timeout_seconds = timeout_seconds
        self.gateway_uri_stem = gateway_uri_stem
        self.rpc_uri_stem = rpc_uri_stem
        self.cid_codec = cid_codec

    def save(self, data: bytes) -> CID:
        response = requests.post(
            f"{self.rpc_uri_stem}/api/v0/add?cid-codec={self.cid_codec}&hash=blake3&pin=true",
            files={"file": data},
        )

        cid_str: str = json.decode(response.content)["Hash"]  # type: ignore
        cid = CID.decode(cid_str)

        return cid

    # Ignore the type error since CID is in the IPLDKind type
    def load(self, id: CID) -> bytes:  # type: ignore
        response = requests.get(
            f"{self.gateway_uri_stem}/ipfs/{str(id)}", timeout=self.timeout_seconds
        )

        return response.content


class IPLDStore(MutableMapping):
    def __init__(self, root_cid: CID | None = None, read_only: bool = True):
        self.store = IPFSStore(cid_codec="raw")
        self.hamt = HAMT(store=IPFSStore(), root_node_id=root_cid)
        self.read_only = read_only
        self.lock = Lock()

    def get_root_cid(self) -> CID:
        if not self.read_only:
            self.lock.acquire(blocking=True)

        cid: CID = self.hamt.root_node_id  # type: ignore

        if not self.read_only:
            self.lock.release()

        return cid

    def make_read_only(self):
        self.lock.acquire(blocking=True)
        self.read_only = True
        self.hamt.make_read_only()
        self.lock.release()

    def enable_write(self):
        self.lock.acquire(blocking=True)
        self.read_only = False
        self.hamt.enable_write()
        self.lock.release()

    def __getitem__(self, key: str):
        if not self.read_only:
            self.lock.acquire(blocking=True)

        try:
            cid: CID = self.hamt[key]  # type: ignore
        # It seems zarr sometimes gets before it writes a key. Raising the KeyError through seems to work, but we do need to release the lock before we do so
        except KeyError:
            if not self.read_only:
                self.lock.release()
            raise KeyError
        data = self.store.load(cid)

        if not self.read_only:
            self.lock.release()

        return data

    def __setitem__(self, key: str, value: bytes):
        if not self.read_only:
            self.lock.acquire(blocking=True)

        cid = self.store.save(value)
        self.hamt[key] = cid

        if not self.read_only:
            self.lock.release()

    def __delitem__(self, key: str):
        if not self.read_only:
            self.lock.acquire(blocking=True)

        del self.hamt[key]

        if not self.read_only:
            self.lock.release()

    def __len__(self):
        if not self.read_only:
            self.lock.acquire(blocking=True)

        length = len(self.hamt)

        if not self.read_only:
            self.lock.release()

        return length

    def __iter__(self):
        if not self.read_only:
            self.lock.acquire(blocking=True)

        hamt_copy = deepcopy(self.hamt)

        if not self.read_only:
            self.lock.release()

        return hamt_copy.__iter__()
