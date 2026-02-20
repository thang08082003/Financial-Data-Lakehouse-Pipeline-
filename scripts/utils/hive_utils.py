"""
Hive Utilities
Các hàm tiện ích để làm việc với Apache Hive
"""

import logging
from typing import List, Dict, Optional, Any
from pyhive import hive
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HiveManager:
    """
    Manager class để tương tác với Hive
    """
    
    def __init__(
        self,
        host: str = 'hive-server',
        port: int = 10000,
        database: str = 'default',
        username: str = 'hive'
    ):
        """
        Khởi tạo Hive manager
        
        Args:
            host: Hive server host
            port: Hive server port
            database: Database name
            username: Username
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        logger.info(f"Initialized Hive manager for {host}:{port}/{database}")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager để tạo connection đến Hive
        
        Yields:
            Hive connection
        """
        conn = None
        try:
            conn = hive.Connection(
                host=self.host,
                port=self.port,
                username=self.username,
                database=self.database
            )
            logger.info("Connected to Hive")
            yield conn
        except Exception as e:
            logger.error(f"Error connecting to Hive: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                logger.info("Closed Hive connection")
    
    def execute_query(
        self,
        query: str,
        fetch: bool = False
    ) -> Optional[List[tuple]]:
        """
        Thực thi một query trong Hive
        
        Args:
            query: SQL query
            fetch: Có fetch results không
            
        Returns:
            List of tuples (results) nếu fetch=True, None otherwise
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Log query (truncated nếu quá dài)
                query_preview = query[:200] + '...' if len(query) > 200 else query
                logger.info(f"Executing query: {query_preview}")
                
                cursor.execute(query)
                
                if fetch:
                    results = cursor.fetchall()
                    logger.info(f"Fetched {len(results)} rows")
                    return results
                else:
                    logger.info("Query executed successfully")
                    return None
                    
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def create_database(self, database_name: str) -> bool:
        """
        Tạo database trong Hive
        
        Args:
            database_name: Tên database
            
        Returns:
            True nếu thành công
        """
        try:
            query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
            self.execute_query(query)
            logger.info(f"Created database: {database_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating database: {str(e)}")
            return False
    
    def create_external_table(
        self,
        table_name: str,
        columns: List[tuple],
        location: str,
        file_format: str = 'TEXTFILE',
        row_format: Optional[str] = None,
        partitioned_by: Optional[List[tuple]] = None
    ) -> bool:
        """
        Tạo external table trong Hive
        
        Args:
            table_name: Tên table
            columns: List of (column_name, column_type) tuples
            location: HDFS location
            file_format: File format (TEXTFILE, ORC, PARQUET, etc.)
            row_format: Row format specification
            partitioned_by: List of partition columns
            
        Returns:
            True nếu thành công
        """
        try:
            # Build column definitions
            col_defs = ', '.join([f"{col[0]} {col[1]}" for col in columns])
            
            # Build query
            query = f"CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} ({col_defs})"
            
            # Add partitioning
            if partitioned_by:
                partition_defs = ', '.join([f"{col[0]} {col[1]}" for col in partitioned_by])
                query += f" PARTITIONED BY ({partition_defs})"
            
            # Add row format
            if row_format:
                query += f" {row_format}"
            
            # Add file format
            query += f" STORED AS {file_format}"
            
            # Add location
            query += f" LOCATION '{location}'"
            
            self.execute_query(query)
            logger.info(f"Created external table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating external table: {str(e)}")
            return False
    
    def add_partition(
        self,
        table_name: str,
        partition_spec: Dict[str, str],
        location: str
    ) -> bool:
        """
        Thêm partition vào table
        
        Args:
            table_name: Tên table
            partition_spec: Dictionary of partition column -> value
            location: HDFS location của partition
            
        Returns:
            True nếu thành công
        """
        try:
            # Build partition specification
            partition_str = ', '.join([f"{k}='{v}'" for k, v in partition_spec.items()])
            
            query = f"ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION ({partition_str}) LOCATION '{location}'"
            
            self.execute_query(query)
            logger.info(f"Added partition to {table_name}: {partition_spec}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding partition: {str(e)}")
            return False
    
    def msck_repair_table(self, table_name: str) -> bool:
        """
        MSCK REPAIR TABLE để tự động discover partitions
        
        Args:
            table_name: Tên table
            
        Returns:
            True nếu thành công
        """
        try:
            query = f"MSCK REPAIR TABLE {table_name}"
            self.execute_query(query)
            logger.info(f"Repaired table partitions: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error repairing table: {str(e)}")
            return False
    
    def show_tables(self, database: Optional[str] = None) -> List[str]:
        """
        Liệt kê các tables trong database
        
        Args:
            database: Database name (default: current database)
            
        Returns:
            List of table names
        """
        try:
            query = f"SHOW TABLES IN {database}" if database else "SHOW TABLES"
            results = self.execute_query(query, fetch=True)
            tables = [row[0] for row in results] if results else []
            logger.info(f"Found {len(tables)} tables")
            return tables
        except Exception as e:
            logger.error(f"Error showing tables: {str(e)}")
            return []
    
    def describe_table(self, table_name: str) -> List[tuple]:
        """
        Describe table structure
        
        Args:
            table_name: Tên table
            
        Returns:
            List of (column_name, data_type, comment) tuples
        """
        try:
            query = f"DESCRIBE {table_name}"
            results = self.execute_query(query, fetch=True)
            logger.info(f"Described table: {table_name}")
            return results or []
        except Exception as e:
            logger.error(f"Error describing table: {str(e)}")
            return []
    
    def drop_table(self, table_name: str, if_exists: bool = True) -> bool:
        """
        Drop table
        
        Args:
            table_name: Tên table
            if_exists: Thêm IF EXISTS clause
            
        Returns:
            True nếu thành công
        """
        try:
            exists_clause = "IF EXISTS " if if_exists else ""
            query = f"DROP TABLE {exists_clause}{table_name}"
            self.execute_query(query)
            logger.info(f"Dropped table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error dropping table: {str(e)}")
            return False
    
    def select_query(
        self,
        table_name: str,
        columns: List[str] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[tuple]:
        """
        Execute SELECT query
        
        Args:
            table_name: Tên table
            columns: List of columns (default: *)
            where: WHERE clause
            limit: LIMIT value
            
        Returns:
            Query results
        """
        try:
            # Build column list
            col_str = ', '.join(columns) if columns else '*'
            
            # Build query
            query = f"SELECT {col_str} FROM {table_name}"
            
            if where:
                query += f" WHERE {where}"
            
            if limit:
                query += f" LIMIT {limit}"
            
            results = self.execute_query(query, fetch=True)
            return results or []
            
        except Exception as e:
            logger.error(f"Error executing select query: {str(e)}")
            return []
    
    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """
        Lấy statistics của table
        
        Args:
            table_name: Tên table
            
        Returns:
            Dictionary chứa statistics
        """
        try:
            # Get count
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            count_result = self.execute_query(count_query, fetch=True)
            row_count = count_result[0][0] if count_result else 0
            
            # Get table info
            describe_result = self.describe_table(table_name)
            num_columns = len([row for row in describe_result if row[0] and not row[0].startswith('#')])
            
            stats = {
                'table_name': table_name,
                'row_count': row_count,
                'num_columns': num_columns,
                'columns': describe_result
            }
            
            logger.info(f"Got stats for {table_name}: {row_count} rows, {num_columns} columns")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting table stats: {str(e)}")
            return {}
    
    def optimize_table(self, table_name: str) -> bool:
        """
        Optimize table bằng cách analyze
        
        Args:
            table_name: Tên table
            
        Returns:
            True nếu thành công
        """
        try:
            query = f"ANALYZE TABLE {table_name} COMPUTE STATISTICS"
            self.execute_query(query)
            logger.info(f"Optimized table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error optimizing table: {str(e)}")
            return False


def main():
    """
    Test Hive utilities
    """
    hive_manager = HiveManager()
    
    # Create database
    hive_manager.create_database('financial_lakehouse')
    
    # Show tables
    tables = hive_manager.show_tables('financial_lakehouse')
    print(f"Tables: {tables}")


if __name__ == '__main__':
    main()
