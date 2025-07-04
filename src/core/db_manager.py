"""
Database connection and insertion module for the ETL pipeline.
Handles SQLite database operations and batch inserts.
"""

import asyncio
import logging
import pandas as pd
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import List, Dict, Any, Hashable


class DatabaseManager:
    """Manages SQLite database operations for the ETL pipeline."""
    
    def __init__(self, db_path: str, table_name: str = "transactions"):
        """
        Initialize DatabaseManager.
        
        Args:
            db_path: Path to the SQLite database file
            table_name: Name of the target table
        """
        self.db_path = db_path
        self.table_name = table_name
        self.logger = logging.getLogger(__name__)
        
        # Database schema
        self.schema = {
            'transaction_id': 'TEXT PRIMARY KEY',
            'user_id': 'TEXT NOT NULL',
            'amount': 'REAL NOT NULL',
            'timestamp': 'TEXT',
            'status': 'TEXT NOT NULL',
            'processed_at': 'TEXT NOT NULL'
        }
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.
        
        Yields:
            sqlite3.Connection: Database connection
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30.0)
            conn.execute("PRAGMA journal_mode=WAL")  # Enable WAL mode for better concurrency
            conn.execute("PRAGMA synchronous=NORMAL")  # Balance between safety and performance
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Database connection error: {str(e)}", exc_info=True)
            raise
        finally:
            if conn:
                conn.close()
    
    def create_table(self) -> bool:
        """
        Create the transactions table if it doesn't exist.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            schema_sql = ', '.join([f"{col} {dtype}" for col, dtype in self.schema.items()])
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                {schema_sql}
            )
            """
            
            with self.get_connection() as conn:
                conn.execute(create_sql)
                conn.commit()
            
            self.logger.info(f"Table '{self.table_name}' created/verified successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create table: {str(e)}", exc_info=True)
            return False
    
    def create_indexes(self) -> bool:
        """
        Create indexes for better query performance.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            indexes = [
                f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_user_id ON {self.table_name}(user_id)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_status ON {self.table_name}(status)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_timestamp ON {self.table_name}(timestamp)",
                f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_amount ON {self.table_name}(amount)"
            ]
            
            with self.get_connection() as conn:
                for index_sql in indexes:
                    conn.execute(index_sql)
                conn.commit()
            
            self.logger.info("Database indexes created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create indexes: {str(e)}", exc_info=True)
            return False
    
    def batch_insert(self, data: List[Dict[Hashable, Any]], batch_size: int = 1000) -> bool:
        """
        Insert data in batches using parameterized queries.
        
        Args:
            data: List of dictionaries containing row data
            batch_size: Number of rows per batch
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not data:
                self.logger.warning("No data provided for batch insert")
                return True
            
            # Prepare INSERT statement
            columns = list(self.schema.keys())
            placeholders = ', '.join(['?' for _ in columns])
            insert_sql = f"INSERT OR REPLACE INTO {self.table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            total_rows = len(data)
            inserted_rows = 0
            
            with self.get_connection() as conn:
                # Process data in batches
                for i in range(0, total_rows, batch_size):
                    batch = data[i:i + batch_size]
                    
                    # Convert dictionaries to tuples in column order
                    batch_values = []
                    for row in batch:
                        try:
                            values = tuple(row.get(col, None) for col in columns)
                            batch_values.append(values)
                        except Exception as e:
                            self.logger.warning(f"Skipping malformed row: {str(e)}")
                            continue
                    
                    if batch_values:
                        conn.executemany(insert_sql, batch_values)
                        inserted_rows += len(batch_values)
                
                conn.commit()
            
            self.logger.info(f"Successfully inserted {inserted_rows}/{total_rows} rows")
            return True
            
        except Exception as e:
            self.logger.error(f"Batch insert failed: {str(e)}", exc_info=True)
            return False
    
    def insert_from_dataframe(self, df: pd.DataFrame, batch_size: int = 1000) -> bool:
        """
        Insert data from a pandas DataFrame.
        
        Args:
            df: DataFrame containing the data to insert
            batch_size: Number of rows per batch
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if df.empty:
                self.logger.warning("DataFrame is empty, nothing to insert")
                return True
            
            # Convert DataFrame to list of dictionaries
            data = df.to_dict('records')
            return self.batch_insert(data, batch_size)
            
        except Exception as e:
            self.logger.error(f"DataFrame insert failed: {str(e)}", exc_info=True)
            return False
    
    async def load_data_from_csv(self, csv_file: str, chunk_size: int = 1000) -> bool:
        """
        Load data from a CSV file into the database using chunked reading.
        
        Args:
            csv_file: Path to the CSV file
            chunk_size: Number of rows to process per chunk
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.logger.info(f"Loading data from {csv_file} into database")
            
            def load_in_thread():
                total_rows = 0
                successful_chunks = 0
                
                try:
                    # Read and insert data in chunks
                    for chunk_num, chunk in enumerate(pd.read_csv(csv_file, chunksize=chunk_size)):
                        try:
                            success = self.insert_from_dataframe(chunk, batch_size=chunk_size)
                            if success:
                                total_rows += len(chunk)
                                successful_chunks += 1
                                self.logger.debug(f"Loaded chunk {chunk_num}: {len(chunk)} rows")
                            else:
                                self.logger.error(f"Failed to load chunk {chunk_num}")
                        except Exception as e:
                            self.logger.error(f"Error loading chunk {chunk_num}: {str(e)}", exc_info=True)
                            continue
                    
                    return total_rows, successful_chunks
                    
                except Exception as e:
                    self.logger.error(f"Error reading CSV file: {str(e)}", exc_info=True)
                    return 0, 0
            
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=1) as executor:
                total_rows, successful_chunks = await loop.run_in_executor(executor, load_in_thread)
            
            if total_rows > 0:
                self.logger.info(f"Successfully loaded {total_rows} rows from {successful_chunks} chunks")
                
                # Create indexes after loading data
                self.create_indexes()
                return True
            else:
                self.logger.error("No data was loaded into the database")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to load CSV data: {str(e)}", exc_info=True)
            return False
    
    def get_table_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the table.
        
        Returns:
            dict: Table statistics
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
                row_count = cursor.fetchone()[0]
                
                # Get status distribution
                cursor.execute(f"SELECT status, COUNT(*) FROM {self.table_name} GROUP BY status")
                status_dist = dict(cursor.fetchall())
                
                # Get amount statistics
                cursor.execute(f"""
                    SELECT 
                        MIN(amount) as min_amount,
                        MAX(amount) as max_amount,
                        AVG(amount) as avg_amount,
                        SUM(amount) as total_amount
                    FROM {self.table_name}
                """)
                amount_stats = cursor.fetchone()
                
                return {
                    'row_count': row_count,
                    'status_distribution': status_dist,
                    'amount_stats': {
                        'min': amount_stats[0],
                        'max': amount_stats[1],
                        'average': round(amount_stats[2], 2) if amount_stats[2] else 0,
                        'total': round(amount_stats[3], 2) if amount_stats[3] else 0
                    }
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get table stats: {str(e)}", exc_info=True)
            return {}
    
    def clear_table(self) -> bool:
        """
        Clear all data from the table.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.get_connection() as conn:
                conn.execute(f"DELETE FROM {self.table_name}")
                conn.commit()
            
            self.logger.info(f"Table '{self.table_name}' cleared successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to clear table: {str(e)}", exc_info=True)
            return False
    
    def validate_data_integrity(self) -> bool:
        """
        Validate data integrity in the database.
        
        Returns:
            bool: True if validation passes, False otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Check for null values in required fields
                required_fields = ['transaction_id', 'user_id', 'amount', 'status', 'processed_at']
                for field in required_fields:
                    cursor.execute(f"SELECT COUNT(*) FROM {self.table_name} WHERE {field} IS NULL")
                    null_count = cursor.fetchone()[0]
                    if null_count > 0:
                        self.logger.error(f"Found {null_count} null values in required field: {field}")
                        return False
                
                # Check for negative amounts
                cursor.execute(f"SELECT COUNT(*) FROM {self.table_name} WHERE amount < 0")
                negative_count = cursor.fetchone()[0]
                if negative_count > 0:
                    self.logger.error(f"Found {negative_count} negative amounts")
                    return False
                
                # Check for cancelled transactions
                cursor.execute(f"SELECT COUNT(*) FROM {self.table_name} WHERE LOWER(status) = 'cancelled'")
                cancelled_count = cursor.fetchone()[0]
                if cancelled_count > 0:
                    self.logger.error(f"Found {cancelled_count} cancelled transactions")
                    return False
                
                self.logger.info("Database data integrity validation passed")
                return True
                
        except Exception as e:
            self.logger.error(f"Data integrity validation failed: {str(e)}", exc_info=True)
            return False