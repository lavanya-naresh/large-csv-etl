"""
Data processing and transformation module for the ETL pipeline.
Handles data cleaning, validation, and transformation operations.
"""

import logging
import pandas as pd
from datetime import datetime, timezone
from typing import Optional


class DataProcessor:
    """Handles data transformation and cleaning operations."""
    
    def __init__(self):
        """Initialize DataProcessor."""
        self.logger = logging.getLogger(__name__)
        
        # Define expected columns
        self.required_columns = ['transaction_id', 'user_id', 'amount', 'timestamp', 'status']
    
    def validate_chunk_schema(self, chunk: pd.DataFrame) -> bool:
        """
        Validate that the chunk has the expected schema.
        
        Args:
            chunk: DataFrame chunk to validate
            
        Returns:
            bool: True if schema is valid, False otherwise
        """
        missing_columns = set(self.required_columns) - set(chunk.columns)
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False
        return True
    
    def clean_and_validate_data(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and validate data in the chunk.
        
        Args:
            chunk: Raw DataFrame chunk
            
        Returns:
            pd.DataFrame: Cleaned chunk
        """
        original_count = len(chunk)
        
        try:
            # Remove rows with null transaction_id or user_id (critical fields)
            chunk = chunk.dropna(subset=['transaction_id', 'user_id'])
            
            # Convert amount to numeric, coercing errors to NaN
            chunk['amount'] = pd.to_numeric(chunk['amount'], errors='coerce')
            
            # Remove rows with invalid amounts (NaN after conversion)
            chunk = chunk.dropna(subset=['amount'])
            
            # Ensure status is string type and handle nulls
            chunk['status'] = chunk['status'].astype(str).fillna('unknown')
            
            # Log data quality issues
            cleaned_count = len(chunk)
            if cleaned_count < original_count:
                self.logger.warning(f"Removed {original_count - cleaned_count} rows due to data quality issues")
            
            return chunk
            
        except Exception as e:
            self.logger.error(f"Error during data cleaning: {str(e)}")
            raise
    
    def apply_business_rules(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Apply business rules for data filtering.
        
        Args:
            chunk: DataFrame chunk to filter
            
        Returns:
            pd.DataFrame: Filtered chunk
        """
        original_count = len(chunk)
        
        try:
            # Remove rows where amount is negative
            chunk = chunk[chunk['amount'] >= 0]
            negative_amount_removed = original_count - len(chunk)
            
            if negative_amount_removed > 0:
                self.logger.info(f"Removed {negative_amount_removed} rows with negative amounts")
            
            # Remove rows where status is "cancelled" (case-insensitive)
            current_count = len(chunk)
            chunk = chunk[chunk['status'].str.lower() != 'cancelled']
            cancelled_removed = current_count - len(chunk)
            
            if cancelled_removed > 0:
                self.logger.info(f"Removed {cancelled_removed} rows with cancelled status")
            
            return chunk
            
        except Exception as e:
            self.logger.error(f"Error applying business rules: {str(e)}")
            raise
    
    def transform_data(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """
        Apply data transformations to the chunk.
        
        Args:
            chunk: DataFrame chunk to transform
            
        Returns:
            pd.DataFrame: Transformed chunk
        """
        try:
            # Standardize status column (lowercase)
            chunk['status'] = chunk['status'].str.lower().str.strip()
            
            # Add processed_at timestamp
            current_utc = datetime.now(timezone.utc).isoformat()
            chunk['processed_at'] = current_utc
            
            # Ensure transaction_id is string type
            chunk['transaction_id'] = chunk['transaction_id'].astype(str)
            
            # Ensure user_id is string type
            chunk['user_id'] = chunk['user_id'].astype(str)
            
            # Round amount to 2 decimal places
            chunk['amount'] = chunk['amount'].round(2)
            
            # Reorder columns for consistency
            column_order = ['transaction_id', 'user_id', 'amount', 'timestamp', 'status', 'processed_at']
            chunk = chunk[column_order]
            
            return chunk
            
        except Exception as e:
            self.logger.error(f"Error during data transformation: {str(e)}")
            raise
    
    def transform_chunk(self, chunk: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        Main transformation function that applies all processing steps.
        
        Args:
            chunk: Raw DataFrame chunk
            
        Returns:
            pd.DataFrame: Processed chunk, or None if processing failed
        """
        try:
            # Validate schema
            if not self.validate_chunk_schema(chunk):
                self.logger.error("Chunk schema validation failed")
                return None
            
            original_rows = len(chunk)
            self.logger.debug(f"Processing chunk with {original_rows} rows")
            
            # Step 1: Clean and validate data
            chunk = self.clean_and_validate_data(chunk)
            if chunk.empty:
                self.logger.warning("Chunk is empty after data cleaning")
                return None
            
            # Step 2: Apply business rules
            chunk = self.apply_business_rules(chunk)
            if chunk.empty:
                self.logger.warning("Chunk is empty after applying business rules")
                return None
            
            # Step 3: Transform data
            chunk = self.transform_data(chunk)
            
            final_rows = len(chunk)
            self.logger.debug(f"Chunk processing complete: {original_rows} -> {final_rows} rows")
            
            return chunk
            
        except Exception as e:
            self.logger.error(f"Chunk transformation failed: {str(e)}", exc_info=True)
            return None
    
    def get_processing_stats(self, original_chunk: pd.DataFrame, 
                           processed_chunk: Optional[pd.DataFrame]) -> dict:
        """
        Calculate processing statistics for a chunk.
        
        Args:
            original_chunk: Original DataFrame chunk
            processed_chunk: Processed DataFrame chunk
            
        Returns:
            dict: Processing statistics
        """
        stats = {
            'original_rows': len(original_chunk),
            'processed_rows': len(processed_chunk) if processed_chunk is not None else 0,
            'rows_removed': 0,
            'success': processed_chunk is not None
        }
        
        if processed_chunk is not None:
            stats['rows_removed'] = stats['original_rows'] - stats['processed_rows']
            
        return stats
    
    def validate_final_data(self, data: pd.DataFrame) -> bool:
        """
        Perform final validation on processed data.
        
        Args:
            data: Final processed DataFrame
            
        Returns:
            bool: True if validation passes, False otherwise
        """
        try:
            # Check for required columns
            if not self.validate_chunk_schema(data):
                return False
            
            # Verify no negative amounts
            if (data['amount'] < 0).any():
                self.logger.error("Final data contains negative amounts")
                return False
            
            # Verify no cancelled transactions
            if (data['status'].str.lower() == 'cancelled').any():
                self.logger.error("Final data contains cancelled transactions")
                return False
            
            # Verify processed_at column exists and is populated
            if 'processed_at' not in data.columns or data['processed_at'].isnull().any():
                self.logger.error("Final data missing processed_at timestamps")
                return False
            
            self.logger.info("Final data validation passed")
            return True
            
        except Exception as e:
            self.logger.error(f"Final data validation failed: {str(e)}")
            return False