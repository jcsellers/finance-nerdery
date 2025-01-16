from zipline.data.bundles import register

from src.custom_ingest import custom_ingest

register("custom_bundle", custom_ingest)
