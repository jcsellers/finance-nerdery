import pytest
from zipline.data import bundles


@pytest.fixture(scope="module", autouse=True)
def setup_bundle():
    """Ensure the custom bundle is ingested."""
    import os
    os.system("python src/bundles/custom_bundle.py")


def test_zipline_ingestion():
    """Test that the custom bundle is ingested correctly."""
    bundle_name = "custom_csv"
    custom_bundle = bundles.load(bundle_name)
    assets = custom_bundle.asset_finder.retrieve_all()
    assert len(assets) > 0, "No assets found in the bundle."
