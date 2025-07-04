"""
File I/O and chunking module for the ETL pipeline.
Handles reading large CSV files in chunks and managing temporary files.
"""

import asyncio
import logging
import pandas as pd
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Callable, Optional
import uuid


class FileManager:
    """Manages file I/O operations and chunking for large CSV files."""
    
    def __init__(self, temp_dir: str = "temp_chunks"):
        """
        Initialize FileManager.
        
        Args:
            temp_dir: Directory for temporary chunk files
        """
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(exist_ok=True)
        self.logger = logging.getLogger(__name__)
        
    def read_csv_chunks(self, file_path: str, chunk_size: int):
        """
        Generator to read CSV file in chunks.
        
        Args:
            file_path: Path to the input CSV file
            chunk_size: Number of rows per chunk
            
        Yields:
            tuple: (chunk_index, pandas.DataFrame)
        """
        try:
            chunk_reader = pd.read_csv(file_path, chunksize=chunk_size)
            for i, chunk in enumerate(chunk_reader):
                yield i, chunk
        except Exception as e:
            self.logger.error(f"Error reading CSV file {file_path}: {str(e)}")
            raise
    
    def save_chunk(self, chunk: pd.DataFrame, chunk_index: int) -> Optional[str]:
        """
        Save a processed chunk to a temporary file.
        
        Args:
            chunk: Processed DataFrame chunk
            chunk_index: Index of the chunk
            
        Returns:
            str: Path to saved file, None if failed
        """
        try:
            filename = f"chunk_{chunk_index:06d}_{uuid.uuid4().hex[:8]}.csv"
            file_path = self.temp_dir / filename
            chunk.to_csv(file_path, index=False)
            self.logger.debug(f"Saved chunk {chunk_index} to {file_path}")
            return str(file_path)
        except Exception as e:
            self.logger.error(f"Failed to save chunk {chunk_index}: {str(e)}")
            return None
    
    def process_single_chunk(self, chunk_data: tuple, transform_func: Callable) -> Optional[str]:
        """
        Process a single chunk with the given transformation function.
        
        Args:
            chunk_data: Tuple of (chunk_index, DataFrame)
            transform_func: Function to transform the chunk
            
        Returns:
            str: Path to processed chunk file, None if failed
        """
        chunk_index, chunk = chunk_data
        try:
            self.logger.debug(f"Processing chunk {chunk_index} with {len(chunk)} rows")
            
            # Transform the chunk
            processed_chunk = transform_func(chunk)
            
            if processed_chunk is not None and not processed_chunk.empty:
                # Save processed chunk
                saved_path = self.save_chunk(processed_chunk, chunk_index)
                if saved_path:
                    self.logger.info(f"Successfully processed chunk {chunk_index}: "
                                   f"{len(processed_chunk)} rows saved")
                    return saved_path
                else:
                    self.logger.warning(f"Failed to save processed chunk {chunk_index}")
            else:
                self.logger.warning(f"Chunk {chunk_index} resulted in empty data after processing")
                
        except Exception as e:
            self.logger.error(f"Error processing chunk {chunk_index}: {str(e)}", exc_info=True)
        
        return None
    
    async def process_chunks_parallel(self, input_file: str, transform_func: Callable,
                                    chunk_size: int, max_workers: int) -> List[str]:
        """
        Process CSV chunks in parallel using ThreadPoolExecutor.
        
        Args:
            input_file: Path to input CSV file
            transform_func: Function to transform each chunk
            chunk_size: Number of rows per chunk
            max_workers: Maximum number of parallel workers
            
        Returns:
            List[str]: Paths to successfully processed chunk files
        """
        processed_files = []
        
        def run_in_thread():
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all chunks for processing
                future_to_chunk = {
                    executor.submit(self.process_single_chunk, chunk_data, transform_func): chunk_data[0]
                    for chunk_data in self.read_csv_chunks(input_file, chunk_size)
                }
                
                # Collect results as they complete
                chunk_results = []
                for future in as_completed(future_to_chunk):
                    chunk_index = future_to_chunk[future]
                    try:
                        result = future.result()
                        if result:
                            chunk_results.append(result)
                    except Exception as e:
                        self.logger.error(f"Chunk {chunk_index} processing failed: {str(e)}")
                
                return chunk_results
        
        # Run the threading operation in an executor to keep it async
        loop = asyncio.get_event_loop()
        processed_files = await loop.run_in_executor(None, run_in_thread)
        
        self.logger.info(f"Processed {len(processed_files)} chunks successfully")
        return processed_files
    
    async def combine_chunks(self, chunk_files: List[str], output_file: str = '') -> str:
        """
        Combine processed chunk files into a single CSV file.
        
        Args:
            chunk_files: List of paths to chunk files
            output_file: Path for the combined output file
            
        Returns:
            str: Path to the combined file
        """
        if not output_file:
            output_file = self.temp_dir / "combined_processed_data.csv"
        
        try:
            self.logger.info(f"Combining {len(chunk_files)} chunk files")
            
            # Sort files to maintain order
            chunk_files.sort()
            
            first_chunk = True
            total_rows = 0
            
            with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                for chunk_file in chunk_files:
                    try:
                        chunk_df = pd.read_csv(chunk_file)
                        
                        # Write header only on first chunk
                        chunk_df.to_csv(outfile, index=False, header=first_chunk, mode='a')
                        total_rows += len(chunk_df)
                        first_chunk = False
                        
                    except Exception as e:
                        self.logger.error(f"Error reading chunk file {chunk_file}: {str(e)}")
                        continue
            
            self.logger.info(f"Combined file created with {total_rows} total rows: {output_file}")
            return str(output_file)
            
        except Exception as e:
            self.logger.error(f"Error combining chunks: {str(e)}")
            raise
    
    def cleanup_temp_files(self):
        """Remove all temporary chunk files."""
        try:
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
                self.logger.info("Temporary files cleaned up")
        except Exception as e:
            self.logger.warning(f"Failed to cleanup temp files: {str(e)}")
    
    def get_file_info(self, file_path: str) -> dict:
        """
        Get basic information about a CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            dict: File information including row count estimate
        """
        try:
            # Get file size
            file_size = Path(file_path).stat().st_size
            
            # Estimate row count by reading first chunk
            sample_chunk = next(pd.read_csv(file_path, chunksize=1000))
            
            return {
                'file_size_mb': round(file_size / (1024 * 1024), 2),
                'columns': list(sample_chunk.columns),
                'sample_rows': len(sample_chunk)
            }
        except Exception as e:
            self.logger.error(f"Error getting file info: {str(e)}")
            return {}