from abc import ABC, abstractmethod
from typing import Dict, MutableMapping, Optional, Union, overload, Iterator, MutableSet, List
from io import BufferedIOBase, BytesIO
from itertools import zip_longest

import aiohttp
import asyncio

from multiformats import CID, multicodec, multibase, multihash, varint
import dag_cbor
from cbor2 import CBORTag
from dag_cbor.encoding import EncodableType as DagCborEncodable
from typing_validation import validate

import requests
from requests.adapters import HTTPAdapter, Retry

from .car import read_car
from .utils import StreamLike


ValueType = Union[bytes, DagCborEncodable]

RawCodec = multicodec.get("raw")
DagPbCodec = multicodec.get("dag-pb")
DagCborCodec = multicodec.get("dag-cbor")


def get_retry_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(connect=5, total=5, backoff_factor=4)
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session


class ContentAddressableStore(ABC):
    @abstractmethod
    def get_raw(self, cid: CID) -> bytes:
        ...

    def get(self, cid: CID) -> ValueType:
        value = self.get_raw(cid)
        if cid.codec == RawCodec:
            return value
        elif cid.codec == DagCborCodec:
            return dag_cbor.decode(value)
        else:
            raise ValueError(f"can't decode CID's codec '{cid.codec.name}'")

    def __contains__(self, cid: CID) -> bool:
        try:
            self.get_raw(cid)
        except KeyError:
            return False
        else:
            return True

    @abstractmethod
    def put_raw(self,
                raw_value: bytes,
                codec: Union[str, int, multicodec.Multicodec]) -> CID:
        ...

    def put(self, value: ValueType) -> CID:
        validate(value, ValueType)
        if isinstance(value, bytes):
            return self.put_raw(value, RawCodec)
        else:
            return self.put_raw(dag_cbor.encode(value), DagCborCodec)

    def normalize_cid(self, cid: CID) -> CID:  # pylint: disable=no-self-use
        return cid

    @overload
    def to_car(self, root: CID, stream: BufferedIOBase) -> int:
        ...

    @overload
    def to_car(self, root: CID, stream: None = None) -> bytes:
        ...

    def to_car(self, root: CID, stream: Optional[BufferedIOBase] = None) -> Union[bytes, int]:
        validate(root, CID)
        validate(stream, Optional[BufferedIOBase])

        if stream is None:
            buffer = BytesIO()
            stream = buffer
            return_bytes = True
        else:
            return_bytes = False

        bytes_written = 0
        header = dag_cbor.encode({"version": 1, "roots": [root]})
        bytes_written += stream.write(varint.encode(len(header)))
        bytes_written += stream.write(header)
        bytes_written += self._to_car(root, stream, set())

        if return_bytes:
            return buffer.getvalue()
        else:
            return bytes_written

    def _to_car(self,
                root: CID,
                stream: BufferedIOBase,
                already_written: MutableSet[CID]) -> int:
        """
            makes a CAR without the header
        """
        bytes_written = 0

        if root not in already_written:
            data = self.get_raw(root)
            cid_bytes = bytes(root)
            bytes_written += stream.write(varint.encode(len(cid_bytes) + len(data)))
            bytes_written += stream.write(cid_bytes)
            bytes_written += stream.write(data)
            already_written.add(root)

            if root.codec == DagCborCodec:
                value = dag_cbor.decode(data)
                for child in iter_links(value):
                    bytes_written += self._to_car(child, stream, already_written)
        return bytes_written

    def import_car(self, stream_or_bytes: StreamLike) -> List[CID]:
        roots, blocks = read_car(stream_or_bytes)
        roots = [self.normalize_cid(root) for root in roots]

        for cid, data, _ in blocks:
            self.put_raw(bytes(data), cid.codec)

        return roots


class MappingCAStore(ContentAddressableStore):
    def __init__(self,
                 mapping: Optional[MutableMapping[str, bytes]] = None,
                 default_hash: Union[str, int, multicodec.Multicodec, multihash.Multihash] = "sha2-256",
                 default_base: Union[str, multibase.Multibase] = "base32",
                 ):
        validate(mapping, Optional[MutableMapping[str, bytes]])
        validate(default_hash, Union[str, int, multicodec.Multicodec, multihash.Multihash])
        validate(default_base, Union[str, multibase.Multibase])

        if mapping is not None:
            self._mapping = mapping
        else:
            self._mapping = {}

        if isinstance(default_hash, multihash.Multihash):
            self._default_hash = default_hash
        else:
            self._default_hash = multihash.Multihash(codec=default_hash)

        if isinstance(default_base, multibase.Multibase):
            self._default_base = default_base
        else:
            self._default_base = multibase.get(default_base)

    def normalize_cid(self, cid: CID) -> CID:
        return cid.set(base=self._default_base, version=1)

    def get_raw(self, cid: CID) -> bytes:
        validate(cid, CID)
        return self._mapping[str(self.normalize_cid(cid))]

    def put_raw(self,
                raw_value: bytes,
                codec: Union[str, int, multicodec.Multicodec]) -> CID:
        validate(raw_value, bytes)
        validate(codec, Union[str, int, multicodec.Multicodec])

        h = self._default_hash.digest(raw_value)
        cid = CID(self._default_base, 1, codec, h)
        self._mapping[str(cid)] = raw_value
        return cid


async def _async_get(host: str, session: aiohttp.ClientSession, cid: CID):
    if cid.codec == DagPbCodec:
        api_method = "/api/v0/cat"
    else:
        api_method = "/api/v0/block/get"
    async with session.post(host + api_method, params={"arg": str(cid)}) as resp:
        return await resp.read()


async def _main_async(keys: List[CID], host: str, d: Dict[CID, bytes]):
    async with aiohttp.ClientSession() as session:
        tasks = [_async_get(host, session, key) for key in keys]
        byte_list = await asyncio.gather(*tasks)
        for i, key in enumerate(keys):
            d[key] = byte_list[i]


class IPFSStore(ContentAddressableStore):
    def __init__(self,
                 host: str,
                 chunker: str = "size-262144",
                 max_nodes_per_level: int = 10000,
                 default_hash: Union[str, int, multicodec.Multicodec, multihash.Multihash] = "sha2-256",
                 ):
        validate(host, str)
        validate(default_hash, Union[str, int, multicodec.Multicodec, multihash.Multihash])

        self._host = host
        self._chunker = chunker
        self._max_nodes_per_level = max_nodes_per_level

        if isinstance(default_hash, multihash.Multihash):
            self._default_hash = default_hash
        else:
            self._default_hash = multihash.Multihash(codec=default_hash)

    def get(self, cid: CID) -> ValueType:
        value = self.get_raw(cid)
        if cid.codec == DagPbCodec:
            return value
        elif cid.codec == DagCborCodec:
            return dag_cbor.decode(value)
        else:
            raise ValueError(f"can't decode CID's codec '{cid.codec.name}'")

    def getitems(self, keys: List[CID]) -> Dict[CID, bytes]:
        ret = {}
        asyncio.run(_main_async(keys, self._host, ret))
        return ret

    def get_raw(self, cid: CID) -> bytes:
        validate(cid, CID)

        session = get_retry_session()

        if cid.codec == DagPbCodec:
            res = session.post(self._host + "/api/v0/cat", params={"arg": str(cid)})
        else:
            res = session.post(self._host + "/api/v0/block/get", params={"arg": str(cid)})
        res.raise_for_status()
        return res.content

    def put(self, value: ValueType) -> CID:
        validate(value, ValueType)
        if isinstance(value, bytes):
            return self.put_raw(value, DagPbCodec)
        else:
            return self.put_raw(dag_cbor.encode(value), DagCborCodec)

    def put_raw(self,
                raw_value: bytes,
                codec: Union[str, int, multicodec.Multicodec],
                should_pin=True) -> CID:
        validate(raw_value, bytes)
        validate(codec, Union[str, int, multicodec.Multicodec])

        if isinstance(codec, str):
            codec = multicodec.get(name=codec)
        elif isinstance(codec, int):
            codec = multicodec.get(code=codec)

        session = get_retry_session()

        if codec == DagPbCodec:
            res = session.post(self._host + "/api/v0/add",
                                params={"pin": False, "chunker": self._chunker},
                                files={"dummy": raw_value})
            res.raise_for_status()
            return CID.decode(res.json()["Hash"])
        else:
            res = session.post(self._host + "/api/v0/dag/put",
                            params={"store-codec": codec.name,
                                    "input-codec": codec.name,
                                    "pin": should_pin,
                                    "hash": self._default_hash.name},
                            files={"dummy": raw_value})
            res.raise_for_status()
            return CID.decode(res.json()["Cid"]["/"])


def iter_links(o: DagCborEncodable) -> Iterator[CID]:
    if isinstance(o, dict):
        for v in o.values():
            yield from iter_links(v)
    elif isinstance(o, list):
        for v in o:
            yield from iter_links(v)
    elif isinstance(o, CID):
        yield o


__all__ = ["ContentAddressableStore", "MappingCAStore", "iter_links"]
