from src.custom_ingest import custom_ingest
from zipline.data.bundles import register

register("custom_bundle", custom_ingest)
