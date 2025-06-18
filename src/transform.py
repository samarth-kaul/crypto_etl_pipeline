# import pandas as pd
# import logging

# def transform_data(raw_data, transform_config):
#     """Transform raw data: clean, filter, and aggregate."""
#     try:
#         # Convert to DataFrame
#         df = pd.DataFrame(raw_data)
        
#         # Select specified columns
#         if transform_config["columns"]:
#             df = df[transform_config["columns"]]
        
#         # Clean data: handle missing values
#         df = df.dropna()
        
#         # Filter data: e.g., min market cap
#         if "min_market_cap" in transform_config:
#             df = df[df["market_cap"] >= transform_config["min_market_cap"]]
        
#         # Perform aggregations
#         if "aggregations" in transform_config:
#             for agg in transform_config["aggregations"]:
#                 group_by = agg["group_by"]
#                 for col_agg in agg["agg_columns"]:
#                     agg_func = col_agg["function"]
#                     output_col = col_agg["output_column"]
#                     df_agg = df.groupby(group_by).agg({col_agg["column"]: agg_func}).reset_index()
#                     df_agg = df_agg.rename(columns={col_agg["column"]: output_col})
#                     df = df.merge(df_agg, on=group_by, how="left")
        
#         logging.info(f"Transformed data: {df.shape[0]} rows, {df.shape[1]} columns")
#         return df
    
#     except Exception as e:
#         logging.error(f"Data transformation failed: {str(e)}")
#         raise



# The transformation will:

# - Remove null values.
# - Filter coins by market cap.
# - Select specific columns.
# - Aggregate data (e.g., average price, total market cap, total volume by symbol).


import pandas as pd
import logging
from typing import Dict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataTransformer:
    """Handles data transformation: cleansing, filtering, and aggregation."""
    
    def __init__(self, transform_config: Dict):
        self.min_market_cap = transform_config['min_market_cap']
        self.columns_to_keep = transform_config['columns_to_keep']
        self.aggregate_config = transform_config.get('aggregate', {})
    
    def transform(self, raw_data: list) -> pd.DataFrame:
        """Transform raw data into a processed DataFrame."""
        try:
            # Convert to DataFrame
            df = pd.DataFrame(raw_data)
            logger.info(f"Raw data loaded into DataFrame with {len(df)} rows")
            
            # Clean data: remove rows with nulls in critical columns
            critical_columns = ['id', 'symbol', 'current_price', 'market_cap']
            df = df.dropna(subset=critical_columns)
            logger.info(f"After cleaning, DataFrame has {len(df)} rows")
            
            # Filter by market cap
            df = df[df['market_cap'] >= self.min_market_cap]
            logger.info(f"After filtering by market cap >= {self.min_market_cap}, DataFrame has {len(df)} rows")
            
            # Select specified columns
            df = df[self.columns_to_keep]
            
            # Aggregate data if configured
            if self.aggregate_config:
                group_by = self.aggregate_config['group_by']
                metrics = self.aggregate_config['metrics']
                agg_dict = {v: 'mean' if k == 'avg_price' else 'sum' for k, v in metrics.items()}
                df = df.groupby(group_by).agg(agg_dict).reset_index()
                logger.info(f"After aggregation by {group_by}, DataFrame has {len(df)} rows")
            
            return df
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise