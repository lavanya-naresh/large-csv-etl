#!/usr/bin/env python3
"""
Large CSV ETL Pipeline
Main entry point for processing large CSV files with chunking, transformation, and database loading.
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Optional

from src.data_processor import DataProcessor
from core.db_manager import DatabaseManager
from src.file_manager import FileManager
from src.util.logger_manager import setup_log_config


class ETLPipeline:
    """Main ETL pipeline orchestrator."""
    
    def __init__(self, input_file: str, db_path: str, chunk_size: int = 1000, 
                 max_workers: int = 4, temp_dir: str = "temp_chunks"):
        """
        Initialize the ETL pipeline.
        
        Args:
            input_file: Path to the input CSV file
            db_path: Path to the SQLite database
            chunk_size: Number of rows per chunk
            max_workers: Maximum number of parallel workers
            temp_dir: Directory for temporary chunk files
        """
        self.input_file = input_file
        self.db_path = db_path
        self.chunk_size = chunk_size
        self.max_workers = max_workers
        self.temp_dir = Path(temp_dir)
        
        # Initialize components
        self.file_manager = FileManager(temp_dir)
        self.data_processor = DataProcessor()
        self.db_manager = DatabaseManager(db_path)
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
    async def run(self) -> bool:
        """
        Execute the complete ETL pipeline.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.logger.info(f"Starting ETL pipeline for {self.input_file}")
            self.logger.info(f"Chunk size: {self.chunk_size}, Max workers: {self.max_workers}")
            
            # Ensure temp directory exists
            self.temp_dir.mkdir(exist_ok=True)
            
            # Initialize database
            self.db_manager.create_table()
            
            # Process chunks in parallel
            processed_files = await self.file_manager.process_chunks_parallel(
                self.input_file, 
                self.data_processor.transform_chunk,
                self.chunk_size,
                self.max_workers
            )
            
            if not processed_files:
                self.logger.error("No chunks were successfully processed")
                return False
            
            # Combine processed chunks
            final_file = await self.file_manager.combine_chunks(processed_files)
            self.logger.info(f"Combined processed data saved to: {final_file}")
            
            # Load to database
            success = await self.db_manager.load_data_from_csv(final_file, self.chunk_size)
            
            if success:
                self.logger.info("ETL pipeline completed successfully")
                # Cleanup temp files
                self.file_manager.cleanup_temp_files()
                return True
            else:
                self.logger.error("Database loading failed")
                return False
                
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {str(e)}", exc_info=True)
            return False


def main():
    """Main entry point with CLI interface."""
    parser = argparse.ArgumentParser(description="Large CSV ETL Pipeline")
    parser.add_argument("input_file", help="Path to input CSV file")
    parser.add_argument("--db-path", default="transactions.db", 
                       help="Path to SQLite database (default: transactions.db)")
    parser.add_argument("--chunk-size", type=int, default=1000,
                       help="Chunk size for processing (default: 1000)")
    parser.add_argument("--max-workers", type=int, default=4,
                       help="Maximum number of parallel workers (default: 4)")
    parser.add_argument("--temp-dir", default="temp_chunks",
                       help="Temporary directory for chunk files (default: temp_chunks)")
    parser.add_argument("--log-level", default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level (default: INFO)")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_log_config(args.log_level)
    
    # Validate input file
    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' not found")
        sys.exit(1)
    
    # Create and run pipeline
    pipeline = ETLPipeline(
        input_file=args.input_file,
        db_path=args.db_path,
        chunk_size=args.chunk_size,
        max_workers=args.max_workers,
        temp_dir=args.temp_dir
    )
    
    # Run the pipeline
    success = asyncio.run(pipeline.run())
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()