from math import ceil
from typing import Iterator, Union

import dag_cbor
from multiformats import CID, multicodec, multihash


class MultiformatsBlock:
    def __init__(self, value: bytes, codec: Union[str, int, multicodec.Multicodec, multihash.Multihash], hasher: str):
        if isinstance(hasher, multihash.Multihash):
            _hasher = hasher
        else:
            _hasher = multihash.Multihash(codec=hasher)
        if codec == "dag-cbor":
            bytes_value = dag_cbor.encode(value)
        elif codec == "raw":
            bytes_value = value
        else:
            raise NotImplementedError("only dag-cbor and raw codecs curently available")
        h = _hasher.digest(bytes_value)
        cid = CID("base32", 1, codec, h)
        self.cid = cid
        self.bytes_value = bytes_value


def balanced(parts, hasher, codec, limit=1000) -> Iterator[MultiformatsBlock]:
    parts = [part for part in parts]
    if len(parts) > limit:
        size = ceil(len(parts) / ceil(len(parts) / limit))
        subparts = []
        while parts:
            chunk = parts[:size]
            parts = parts[size:]
            length = sum([l[0] for l in chunk])
            for block in balanced(chunk, hasher, codec, limit):
                yield block
                last = block
            subparts.append([length, last.cid])
        parts = subparts
    yield MultiformatsBlock(parts, codec, hasher)


def from_bytes(input_bytes, sub_chunk_length=128000, hasher="sha2-256", codec="dag-cbor", algo=balanced) -> Iterator[MultiformatsBlock]:
    parts = []
    for i in range(0, len(input_bytes), sub_chunk_length):
        split_bytes = input_bytes[i:i+sub_chunk_length]
        block = MultiformatsBlock(split_bytes, "raw", hasher)
        yield block
        parts.append([len(split_bytes), block.cid])
    yield from algo(parts, hasher, codec)


def read(cid, get, offset=0, end=None) -> bytes:
    res = get(cid)
    if type(res) == bytes:
        return res[offset:end]
    else:
        total_length = 0
        ret_value = bytes()
        for (length, sub_cid) in res:
            if end and total_length > end:
                return ret_value
            else:
                ret_value += read(
                    sub_cid["/"], get=get,
                    offset=(offset - total_length) if offset > total_length else 0,
                    end=end if end is None else end - total_length)
                total_length += length
        return ret_value
