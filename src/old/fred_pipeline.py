import logging
import os


class FredPipeline:
    def __init__(self, fred_fetcher, output_dir, fred_settings):
        """
        Initialize the FredPipeline.

        :param fred_fetcher: Instance of FredFetcher.
        :param output_dir: Directory where output files will be saved.
        :param fred_settings: Dictionary of FRED-specific settings.
        """
        self.fred_fetcher = fred_fetcher
        self.output_dir = output_dir
        self.fred_settings = fred_settings

    def process_fred(self, series_id):
        """
        Process a FRED series by fetching, transforming, and saving the data.

        :param series_id: FRED series ID.
        """
        logger = logging.getLogger(__name__)
        logger.debug(f"Entering process_fred for series_id: {series_id}")

        settings = self.fred_settings.get(series_id, {})
        if not settings:
            logger.error(
                f"No configuration found for FRED series ID {series_id}. Skipping."
            )
            return

        start_date = settings.get("start_date", "2020-01-01")
        end_date = settings.get("end_date", "current")
        alias = settings.get("alias", series_id)

        logger.info(
            f"Fetching FRED data for {series_id} ({alias}) from {start_date} to {end_date}."
        )
        try:
            df = self.fred_fetcher.fetch_data(series_id, start_date, end_date)
            logger.debug(
                f"Fetched data for {series_id}:\n{df.head() if not df.empty else 'No data fetched.'}"
            )
            if df.empty:
                logger.warning(
                    f"No data available for FRED series ID {series_id}. Skipping."
                )
                return

            ohlcv_df = self.fred_fetcher.transform_to_ohlcv(df)
            output_path = os.path.join(self.output_dir, f"{alias}.csv")
            logger.info(f"Saving FRED data for {alias} to {output_path}.")
            ohlcv_df.to_csv(output_path, index_label="Date")
            logger.info(f"Saved FRED data for {alias} to {output_path}.")
        except Exception as e:
            logger.error(
                f"Error processing FRED data for {series_id}: {e}", exc_info=True
            )
