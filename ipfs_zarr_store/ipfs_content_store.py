from typing import Dict, Union, Iterator, List

import aiohttp
import asyncio

from multiformats import CID, multicodec, multihash
import cbor2
from dag_cbor.encoding import EncodableType as DagCborEncodable
from typing_validation import validate

import requests
from requests.adapters import HTTPAdapter, Retry


ValueType = Union[bytes, DagCborEncodable]

DagPbCodec = multicodec.get("dag-pb")
DagCborCodec = multicodec.get("dag-cbor")


def get_retry_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(connect=5, total=5, backoff_factor=4)
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session


def default_encoder(encoder, value):
    encoder.encode(cbor2.CBORTag(42,  b'\x00' + bytes(value)))


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


class IPFSContentStore:
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
        print('thing')
        if isinstance(default_hash, multihash.Multihash):
            self._default_hash = default_hash
        else:
            self._default_hash = multihash.Multihash(codec=default_hash)

    def __contains__(self, cid: CID) -> bool:
        try:
            self.get_raw(cid)
        except KeyError:
            return False
        else:
            return True

    def get(self, cid: CID) -> ValueType:
        value = self.get_raw(cid)
        if cid.codec == DagPbCodec:
            return value
        elif cid.codec == DagCborCodec:
            return cbor2.loads(value)
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
            return self.put_raw(cbor2.dumps(value, default=default_encoder), DagCborCodec)
            

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
            print(CID.decode(res.json()["Hash"]))
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
