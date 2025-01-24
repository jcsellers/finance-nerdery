import logging

import pandas as pd


class DataMerger:
    @staticmethod
    @staticmethod
    def merge_datasets(yahoo_data, fred_data_dict):
        """Align FRED data to the Yahoo Finance timeline and merge with Yahoo data."""
        logging.info(
            "Starting brute-force alignment of FRED data to Yahoo Finance timeline"
        )

        # Ensure Yahoo data index is timezone-naive
        yahoo_data.index = yahoo_data.index.tz_localize(None)
        logging.debug(
            f"Yahoo data index after tz normalization: {yahoo_data.index[:5]}"
        )

        # Initialize a copy of Yahoo data to retain all merged data
        merged_data = yahoo_data.copy()

        for series_name, fred_data in fred_data_dict.items():
            logging.debug(f"Processing FRED series: {series_name}")

            # Normalize FRED index and remove timezone information
            fred_data.index = (
                pd.to_datetime(fred_data.index).normalize().tz_localize(None)
            )
            logging.debug(
                f"Normalized FRED index for {series_name}: {fred_data.index[:5]}"
            )

            # Create an aligned DataFrame for this series
            aligned_data = pd.DataFrame(index=yahoo_data.index)
            matches, misses = 0, 0

            # Brute-force alignment: Find each Yahoo date in FRED and copy the value
            for date in yahoo_data.index:
                if date in fred_data.index:
                    if "Value" in fred_data.columns:
                        aligned_data.loc[date, f"Value_{series_name}"] = fred_data.loc[
                            date, "Value"
                        ]
                        matches += 1
                        logging.debug(
                            f"Matched date {date} (Yahoo) with {date} (FRED) for {series_name}, value: {fred_data.loc[date, 'Value']}"
                        )
                    else:
                        logging.error(
                            f"Column 'Value' not found in FRED data for {series_name}. Available columns: {fred_data.columns}"
                        )
                        break
                else:
                    aligned_data.loc[date, f"Value_{series_name}"] = None
                    misses += 1

            logging.info(f"{series_name}: {matches} matches, {misses} misses")

            # Forward-fill and backward-fill missing values
            aligned_data.ffill(inplace=True)
            aligned_data.bfill(inplace=True)
            logging.debug(
                f"Final aligned data for {series_name}: {aligned_data.head()}"
            )

            # Concatenate the aligned data into the merged dataset
            merged_data = pd.concat([merged_data, aligned_data], axis=1)

            logging.info(f"Final merged dataset shape: {merged_data.shape}")
            logging.debug(f"Final merged dataset preview: {merged_data.head()}")
        return merged_data
