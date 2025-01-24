import logging
import os

from synthetic_data_generator import SyntheticDataGenerator


class SyntheticPipeline:
    def __init__(self, output_dir, synthetic_settings):
        self.output_dir = output_dir
        self.synthetic_settings = synthetic_settings

    def process_synthetic(self, ticker):
        logger = logging.getLogger(__name__)
        logger.debug(f"Entering process_synthetic for ticker: {ticker}")

        settings = self.synthetic_settings.get(ticker, {})
        if not settings:
            logger.error(
                f"No configuration found for synthetic ticker {ticker}. Skipping."
            )
            return

        start_date = settings.get("start_date")
        end_date = settings.get("end_date")
        data_type = settings.get("data_type", "linear")
        start_value = settings.get("start_value", 1.0)
        growth_rate = settings.get("growth_rate", 0.01)

        logger.info(
            f"Generating synthetic data for {ticker} from {start_date} to {end_date}."
        )
        try:
            generator = SyntheticDataGenerator(
                start_date=start_date,
                end_date=end_date,
                ticker=ticker,
                data_type=data_type,
                start_value=start_value,
                growth_rate=growth_rate,
            )
            df = generator.generate()

            assert len(df) > 0, f"Generated DataFrame for {ticker} is empty."
            assert (
                df.index.is_monotonic_increasing
            ), f"Index for {ticker} is not sorted."

            output_path = os.path.join(self.output_dir, f"{ticker}.csv")
            logger.info(f"Saving synthetic data for {ticker} to {output_path}.")
            df.to_csv(output_path, index_label="Date")
            logger.info(f"Saved synthetic data for {ticker} to {output_path}.")
        except Exception as e:
            logger.error(
                f"Error generating synthetic data for {ticker}: {e}", exc_info=True
            )
        logger.debug(f"Exiting process_synthetic for ticker: {ticker}")
