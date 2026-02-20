"""
Configuration Management
Quản lý cấu hình cho toàn bộ pipeline
"""

import os
from dataclasses import dataclass
from typing import List
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class APIConfig:
    """Configuration cho API keys"""
    polygon_api_key: str
    alpha_vantage_api_key: str
    sec_user_agent: str


@dataclass
class HDFSConfig:
    """Configuration cho Hadoop HDFS"""
    namenode_host: str
    namenode_port: int
    namenode_url: str
    raw_data_path: str
    processed_data_path: str
    
    @property
    def hdfs_url(self) -> str:
        return f"hdfs://{self.namenode_host}:{self.namenode_port}"


@dataclass
class HiveConfig:
    """Configuration cho Apache Hive"""
    host: str
    port: int
    database: str
    username: str
    
    @property
    def connection_string(self) -> str:
        return f"{self.host}:{self.port}/{self.database}"


@dataclass
class SparkConfig:
    """Configuration cho Apache Spark"""
    master_url: str
    app_name: str
    executor_memory: str
    driver_memory: str
    executor_cores: int
    
    def get_spark_config(self) -> dict:
        return {
            'spark.executor.memory': self.executor_memory,
            'spark.driver.memory': self.driver_memory,
            'spark.executor.cores': str(self.executor_cores),
        }


@dataclass
class PostgreSQLConfig:
    """Configuration cho PostgreSQL"""
    host: str
    port: int
    database: str
    username: str
    password: str
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


@dataclass
class PipelineConfig:
    """Configuration cho data pipeline"""
    tickers: List[str]
    start_date: str
    end_date: str
    batch_size: int
    max_retries: int
    retry_delay: int  # seconds


class Config:
    """
    Main configuration class
    Centralized configuration management
    """
    
    def __init__(self):
        # API Configuration
        self.api = APIConfig(
            polygon_api_key=os.getenv('POLYGON_API_KEY', ''),
            alpha_vantage_api_key=os.getenv('ALPHA_VANTAGE_API_KEY', ''),
            sec_user_agent=os.getenv('SEC_USER_AGENT', 'Financial Lakehouse contact@example.com')
        )
        
        # HDFS Configuration
        self.hdfs = HDFSConfig(
            namenode_host=os.getenv('HDFS_NAMENODE_HOST', 'namenode'),
            namenode_port=int(os.getenv('HDFS_NAMENODE_PORT', 9000)),
            namenode_url=os.getenv('HDFS_NAMENODE_URL', 'http://namenode:9870'),
            raw_data_path=os.getenv('DATA_RAW_PATH', '/data/raw'),
            processed_data_path=os.getenv('DATA_PROCESSED_PATH', '/data/processed')
        )
        
        # Hive Configuration
        self.hive = HiveConfig(
            host=os.getenv('HIVE_SERVER_HOST', 'hive-server'),
            port=int(os.getenv('HIVE_SERVER_PORT', 10000)),
            database=os.getenv('HIVE_DATABASE', 'financial_lakehouse'),
            username=os.getenv('HIVE_USERNAME', 'hive')
        )
        
        # Spark Configuration
        self.spark = SparkConfig(
            master_url=os.getenv('SPARK_MASTER_URL', 'spark://spark-master:7077'),
            app_name=os.getenv('SPARK_APP_NAME', 'Financial-Lakehouse-Pipeline'),
            executor_memory=os.getenv('SPARK_EXECUTOR_MEMORY', '4g'),
            driver_memory=os.getenv('SPARK_DRIVER_MEMORY', '2g'),
            executor_cores=int(os.getenv('SPARK_EXECUTOR_CORES', 2))
        )
        
        # PostgreSQL Configuration
        self.postgres = PostgreSQLConfig(
            host=os.getenv('POSTGRES_HOST', 'postgres'),
            port=int(os.getenv('POSTGRES_PORT', 5432)),
            database=os.getenv('POSTGRES_DB', 'airflow_db'),
            username=os.getenv('POSTGRES_USER', 'airflow'),
            password=os.getenv('POSTGRES_PASSWORD', 'airflow')
        )
        
        # Pipeline Configuration
        tickers_str = os.getenv('TICKERS', 'AAPL,GOOGL,MSFT,AMZN,TSLA')
        self.pipeline = PipelineConfig(
            tickers=tickers_str.split(','),
            start_date=os.getenv('START_DATE', '2024-01-01'),
            end_date=os.getenv('END_DATE', '2024-12-31'),
            batch_size=int(os.getenv('BATCH_SIZE', 10)),
            max_retries=int(os.getenv('MAX_RETRIES', 3)),
            retry_delay=int(os.getenv('RETRY_DELAY', 60))
        )
    
    def validate(self) -> bool:
        """
        Validate configuration
        
        Returns:
            True if configuration is valid
        """
        errors = []
        
        # Validate API keys
        if not self.api.polygon_api_key:
            errors.append("POLYGON_API_KEY is not set")
        if not self.api.alpha_vantage_api_key:
            errors.append("ALPHA_VANTAGE_API_KEY is not set")
        
        # Validate HDFS
        if not self.hdfs.namenode_host:
            errors.append("HDFS_NAMENODE_HOST is not set")
        
        # Validate tickers
        if not self.pipeline.tickers:
            errors.append("TICKERS list is empty")
        
        if errors:
            for error in errors:
                print(f"Configuration Error: {error}")
            return False
        
        return True
    
    def __repr__(self) -> str:
        """String representation of configuration"""
        return f"""
Financial Lakehouse Pipeline Configuration
==========================================
API:
  - Polygon API Key: {'***' + self.api.polygon_api_key[-4:] if self.api.polygon_api_key else 'Not Set'}
  - Alpha Vantage Key: {'***' + self.api.alpha_vantage_api_key[-4:] if self.api.alpha_vantage_api_key else 'Not Set'}

HDFS:
  - NameNode: {self.hdfs.hdfs_url}
  - Raw Data Path: {self.hdfs.raw_data_path}
  - Processed Data Path: {self.hdfs.processed_data_path}

Hive:
  - Connection: {self.hive.connection_string}

Spark:
  - Master: {self.spark.master_url}
  - Executor Memory: {self.spark.executor_memory}
  - Driver Memory: {self.spark.driver_memory}

PostgreSQL:
  - Host: {self.postgres.host}:{self.postgres.port}
  - Database: {self.postgres.database}

Pipeline:
  - Tickers: {', '.join(self.pipeline.tickers[:5])}{'...' if len(self.pipeline.tickers) > 5 else ''}
  - Date Range: {self.pipeline.start_date} to {self.pipeline.end_date}
        """


# Global configuration instance
config = Config()


if __name__ == '__main__':
    # Test configuration
    print(config)
    print(f"\nConfiguration valid: {config.validate()}")
