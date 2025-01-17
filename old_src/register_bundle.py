from custom_ingest import custom_ingest
from zipline.data.bundles import register

# Register the custom bundle
register("custom_bundle", custom_ingest)

# Instructions:
# Run the following command to ingest the custom bundle after registering:
# zipline ingest --bundle custom_bundle
