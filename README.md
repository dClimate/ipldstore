This repository should be considered as experimental.

## examples

### storing on IPFS

Store data on (local) IPFS node:
```python
>>> import ipfs_zarr_store
>>> import xarray as xr
>>> ds = xr.Dataset({"a": ("a", [1, 2, 3])})
>>> m = ipfs_zarr_store.get_ipfs_mapper()
>>> ds.to_zarr(m, encoding={"a": {"compressor": None}}, consolidated=False)   # doctest: +SKIP
<xarray.backends.zarr.ZarrStore object at 0x...>
>>> print(m.freeze())   # doctest: +SKIP
bafyreidn66mk3fktszrfwayonvpq6y3agtnb5e5o22ivof5tgikbxt7k6u

```
(this example does only work if there's a local IPFS node running)

#### Retrieving from IPFS

Since we generate a correctly formatted Zarr, we can read a Zarr on IPLD back into Xarray.
This is as simple as telling Xarray to open the IPLD mapper as the file URI. To correctly format this mapper,
we must decode the hash returned after `to_zarr` into the `base32` string representation (see below)),
then set our mapper's root to that.

Note we use a different hash below to show a more typical N-Dimensional dataset.

```python
>>> import xarray as xr
>>> from multiformats import CID
>>> ipld_mapper = ipfs_zarr_store.get_ipfs_mapper()
>>> cid_obj = CID.decode("bafyreidjxhcilm5r227in4tvrjujawad4n7pydxk543ez53ttx6jieilc4")
>>> cid_obj
CID('base32', 1, 'dag-cbor', '122069b9c485b3b1d6be86f2758a68905803e37efc0eeaef364cf7739dfc94110b17')
>>> ipld_mapper.set_root(cid_obj)  # doctest: +SKIP
>>> z = xr.open_zarr(ipld_mapper, consolidated=False)  # doctest: +SKIP
>>> z  # doctest: +SKIP
<xarray.Dataset>
Dimensions:     (latitude: 721, longitude: 1440, valid_time: 8760)
Coordinates:
  * latitude    (latitude) float64 90.0 89.75 89.5 89.25 ... -89.5 -89.75 -90.0
  * longitude   (longitude) float64 0.0 0.25 0.5 0.75 ... 359.2 359.5 359.8
  * time        (time) datetime64[ns] 1990-01-01 .... 1992-02-06
Data variables:
    tmax        (time, longitude, latitude) float32 dask.array<chunksize=(2190, 1440, 4), meta=np.ndarray>

```
(this example does only work if there's a local IPFS node running and is able to find the referenced root)

We're now able to query, analyze, and otherwise manipulate the Zarr just like a normal Xarray Dataset.
