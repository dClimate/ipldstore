import os
import shutil
import tempfile

import pytest
import numpy as np
import pandas as pd
import xarray as xr
from ipldstore import IPLDStore
from multiformats import CID


@pytest.fixture
def random_zarr_dataset():
    """Creates a random xarray Dataset and saves it to a temporary zarr store.

    Returns:
        tuple: (dataset_path, expected_data)
            - dataset_path: Path to the zarr store
            - expected_data: The original xarray Dataset for comparison
    """
    # Create temporary directory for zarr store
    temp_dir = tempfile.mkdtemp()
    zarr_path = os.path.join(temp_dir, "test.zarr")

    # Coordinates of the random data
    times = pd.date_range("2024-01-01", periods=10)
    lats = np.linspace(-90, 90, 18)
    lons = np.linspace(-180, 180, 36)

    # Create random variables with different shapes
    temp = np.random.randn(len(times), len(lats), len(lons))
    precip = np.random.gamma(2, 0.5, size=(len(times), len(lats), len(lons)))

    # Create the dataset
    ds = xr.Dataset(
        {
            "temp": (
                ["time", "lat", "lon"],
                temp,
                {"units": "celsius", "long_name": "Surface Temperature"},
            ),
            "precip": (
                ["time", "lat", "lon"],
                precip,
                {"units": "mm/day", "long_name": "Daily Precipitation"},
            ),
        },
        coords={
            "time": times,
            "lat": ("lat", lats, {"units": "degrees_north"}),
            "lon": ("lon", lons, {"units": "degrees_east"}),
        },
        attrs={"description": "Test dataset with random weather data"},
    )

    ds.to_zarr(zarr_path, mode="w")

    yield zarr_path, ds

    # Cleanup
    shutil.rmtree(temp_dir)


def test_upload_then_read(random_zarr_dataset: tuple[str, xr.Dataset]):
    zarr_path, expected_ds = random_zarr_dataset
    test_ds = xr.open_zarr(zarr_path)

    ipld_store = IPLDStore()
    ipld_store.make_read_only()
    ipld_store.enable_write()
    test_ds.to_zarr(store=ipld_store, mode="w")
    final_root_cid: CID = ipld_store.get_root_cid()

    ipld_store2 = IPLDStore(root_cid=final_root_cid)
    loaded_ds = xr.open_zarr(store=ipld_store2)

    xr.testing.assert_identical(loaded_ds, expected_ds)

    assert "temp" in loaded_ds
    assert "precip" in loaded_ds
    assert loaded_ds.temp.attrs["units"] == "celsius"

    assert loaded_ds.temp.shape == expected_ds.temp.shape

    # Mostly for complete code coverage's sake
    zarr_file = list(ipld_store)[0]
    del ipld_store[zarr_file]
