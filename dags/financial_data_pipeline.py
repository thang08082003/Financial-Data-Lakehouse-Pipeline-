"""
Financial Data Pipeline DAG
Main Airflow DAG để orchestrate toàn bộ data pipeline
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

# Import extractors
import sys
sys.path.append('/opt/airflow/scripts')

from extractors.polygon_extractor import PolygonExtractor
from extractors.alpha_vantage_extractor import AlphaVantageExtractor
from extractors.sec_extractor import SECExtractor
from utils.hdfs_utils import HDFSManager
from utils.config import config

# =====================================
# DAG Configuration
# =====================================

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    'financial_data_pipeline',
    default_args=default_args,
    description='End-to-end financial data lakehouse pipeline',
    schedule_interval='0 6 * * *',  # Run daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['financial', 'etl', 'lakehouse'],
)

# =====================================
# Python Functions for Tasks
# =====================================

def extract_polygon_data(**context):
    """Extract data from Polygon API"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    tickers = config.pipeline.tickers
    
    print(f"Extracting Polygon data for {len(tickers)} tickers on {execution_date}")
    
    extractor = PolygonExtractor()
    
    from_date = (context['execution_date'] - timedelta(days=7)).strftime('%Y-%m-%d')
    to_date = execution_date
    
    files = extractor.extract_multiple_tickers(
        tickers=tickers,
        from_date=from_date,
        to_date=to_date,
        output_dir='/tmp/polygon'
    )
    
    print(f"Extracted {len(files)} files from Polygon API")
    return files


def extract_alpha_vantage_data(**context):
    """Extract data from Alpha Vantage API"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    tickers = config.pipeline.tickers[:5]  # Limit để tránh rate limit
    
    print(f"Extracting Alpha Vantage data for {len(tickers)} tickers on {execution_date}")
    
    extractor = AlphaVantageExtractor()
    
    files = extractor.extract_multiple_tickers(
        tickers=tickers,
        include_indicators=False,  # Sẽ tính sau bằng Spark
        output_dir='/tmp/alpha_vantage'
    )
    
    print(f"Extracted {len(files)} files from Alpha Vantage API")
    return files


def extract_sec_data(**context):
    """Extract data from SEC API"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    tickers = config.pipeline.tickers
    
    print(f"Extracting SEC data for {len(tickers)} tickers on {execution_date}")
    
    extractor = SECExtractor()
    
    files = extractor.extract_multiple_tickers(
        tickers=tickers,
        output_dir='/tmp/sec'
    )
    
    print(f"Extracted {len(files)} files from SEC API")
    return files


def extract_news_sentiment(**context):
    """Extract news sentiment from Alpha Vantage"""
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    tickers = config.pipeline.tickers[:10]  # Limit để tránh rate limit
    
    print(f"Extracting news sentiment for {len(tickers)} tickers")
    
    extractor = AlphaVantageExtractor()
    
    files = []
    for ticker in tickers:
        try:
            # Get news for each ticker
            import json
            import time
            
            news_data = extractor.get_news_sentiment(tickers=ticker, limit=50)
            
            filename = f'/tmp/news_sentiment/news_{ticker}_{execution_date}.json'
            os.makedirs(os.path.dirname(filename), exist_ok=True)
            
            with open(filename, 'w') as f:
                json.dump(news_data, f, indent=2)
            
            files.append(filename)
            time.sleep(12)  # Rate limiting
            
        except Exception as e:
            print(f"Error extracting news for {ticker}: {str(e)}")
            continue
    
    print(f"Extracted news sentiment to {len(files)} files")
    return files


def upload_to_hdfs(source_dir: str, hdfs_base_path: str, **context):
    """Upload extracted files to HDFS"""
    execution_date = context['execution_date']
    
    print(f"Uploading files from {source_dir} to HDFS {hdfs_base_path}")
    
    hdfs_manager = HDFSManager()
    
    uploaded_files = []
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            if file.endswith('.json'):
                local_path = os.path.join(root, file)
                
                # Create partitioned HDFS path
                hdfs_path = hdfs_manager.upload_with_partitioning(
                    local_path=local_path,
                    base_hdfs_path=hdfs_base_path,
                    filename=file,
                    date=execution_date,
                    partition_by='date'
                )
                
                if hdfs_path:
                    uploaded_files.append(hdfs_path)
    
    print(f"Uploaded {len(uploaded_files)} files to HDFS")
    return uploaded_files


def upload_polygon_to_hdfs(**context):
    """Upload Polygon data to HDFS"""
    return upload_to_hdfs('/tmp/polygon', '/data/raw/polygon', **context)


def upload_alpha_vantage_to_hdfs(**context):
    """Upload Alpha Vantage data to HDFS"""
    return upload_to_hdfs('/tmp/alpha_vantage', '/data/raw/alpha_vantage', **context)


def upload_sec_to_hdfs(**context):
    """Upload SEC data to HDFS"""
    return upload_to_hdfs('/tmp/sec', '/data/raw/sec', **context)


def upload_news_to_hdfs(**context):
    """Upload news sentiment to HDFS"""
    return upload_to_hdfs('/tmp/news_sentiment', '/data/raw/news_sentiment', **context)


# =====================================
# DAG Tasks
# =====================================

# Task Group: Data Extraction
with TaskGroup('extract_data', tooltip='Extract data from APIs', dag=dag) as extract_group:
    
    extract_polygon = PythonOperator(
        task_id='extract_polygon',
        python_callable=extract_polygon_data,
    )
    
    extract_alpha_vantage = PythonOperator(
        task_id='extract_alpha_vantage',
        python_callable=extract_alpha_vantage_data,
    )
    
    extract_sec = PythonOperator(
        task_id='extract_sec',
        python_callable=extract_sec_data,
    )
    
    extract_news = PythonOperator(
        task_id='extract_news_sentiment',
        python_callable=extract_news_sentiment,
    )

# Task Group: Upload to HDFS
with TaskGroup('upload_to_hdfs', tooltip='Upload data to HDFS', dag=dag) as upload_group:
    
    upload_polygon = PythonOperator(
        task_id='upload_polygon',
        python_callable=upload_polygon_to_hdfs,
    )
    
    upload_alpha_vantage = PythonOperator(
        task_id='upload_alpha_vantage',
        python_callable=upload_alpha_vantage_to_hdfs,
    )
    
    upload_sec = PythonOperator(
        task_id='upload_sec',
        python_callable=upload_sec_to_hdfs,
    )
    
    upload_news = PythonOperator(
        task_id='upload_news',
        python_callable=upload_news_to_hdfs,
    )

# Task Group: Spark Data Cleaning
with TaskGroup('clean_data', tooltip='Clean data with Spark', dag=dag) as clean_group:
    
    clean_polygon = SparkSubmitOperator(
        task_id='clean_polygon_data',
        application='/opt/airflow/scripts/spark_jobs/data_cleaning.py',
        application_args=['polygon', '/data/raw/polygon', '/data/processed/polygon'],
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.executor.memory': '4g',
            'spark.driver.memory': '2g',
        },
    )
    
    clean_alpha_vantage = SparkSubmitOperator(
        task_id='clean_alpha_vantage_data',
        application='/opt/airflow/scripts/spark_jobs/data_cleaning.py',
        application_args=['alpha_vantage', '/data/raw/alpha_vantage', '/data/processed/alpha_vantage'],
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.executor.memory': '4g',
            'spark.driver.memory': '2g',
        },
    )
    
    clean_sec = SparkSubmitOperator(
        task_id='clean_sec_data',
        application='/opt/airflow/scripts/spark_jobs/data_cleaning.py',
        application_args=['sec', '/data/raw/sec', '/data/processed/sec'],
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.executor.memory': '2g',
            'spark.driver.memory': '1g',
        },
    )
    
    clean_news = SparkSubmitOperator(
        task_id='clean_news_sentiment',
        application='/opt/airflow/scripts/spark_jobs/data_cleaning.py',
        application_args=['news_sentiment', '/data/raw/news_sentiment', '/data/processed/news_sentiment'],
        conn_id='spark_default',
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.executor.memory': '2g',
            'spark.driver.memory': '1g',
        },
    )

# Spark: Merge price data
merge_price_data = SparkSubmitOperator(
    task_id='merge_price_data',
    application='/opt/airflow/scripts/spark_jobs/data_transformation.py',
    application_args=['merge_price', '/data/processed/polygon', '/data/processed/alpha_vantage', '/data/processed/price_merged'],
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '4g',
        'spark.driver.memory': '2g',
    },
    dag=dag,
)

# Spark: Calculate technical indicators
calculate_indicators = SparkSubmitOperator(
    task_id='calculate_technical_indicators',
    application='/opt/airflow/scripts/spark_jobs/data_transformation.py',
    application_args=['calc_indicators', '/data/processed/price_merged', '/data/processed/price_with_indicators'],
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '4g',
        'spark.driver.memory': '2g',
    },
    dag=dag,
)

# Spark: Aggregate sentiment
aggregate_sentiment = SparkSubmitOperator(
    task_id='aggregate_daily_sentiment',
    application='/opt/airflow/scripts/spark_jobs/data_transformation.py',
    application_args=['agg_sentiment', '/data/processed/news_sentiment', '/data/processed/sentiment_aggregated'],
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '2g',
        'spark.driver.memory': '1g',
    },
    dag=dag,
)

# Spark: Create master dataset
create_master_dataset = SparkSubmitOperator(
    task_id='create_master_dataset',
    application='/opt/airflow/scripts/spark_jobs/data_transformation.py',
    application_args=['create_master', '/data/processed/price_with_indicators', '/data/processed/sentiment_aggregated', '/data/processed/sec', '/data/processed/master_dataset'],
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '4g',
        'spark.driver.memory': '2g',
    },
    dag=dag,
)

# Spark: Analyze correlation
analyze_correlation = SparkSubmitOperator(
    task_id='analyze_sentiment_correlation',
    application='/opt/airflow/scripts/spark_jobs/sentiment_analysis.py',
    application_args=['correlation', '/data/processed/master_dataset', '/data/analysis/correlation'],
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '4g',
        'spark.driver.memory': '2g',
    },
    dag=dag,
)

# Spark: Detect sentiment events
detect_events = SparkSubmitOperator(
    task_id='detect_sentiment_events',
    application='/opt/airflow/scripts/spark_jobs/sentiment_analysis.py',
    application_args=['detect_events', '/data/processed/master_dataset', '/data/analysis/events', '10', '0.5'],
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '4g',
        'spark.driver.memory': '2g',
    },
    dag=dag,
)

# Spark: Create summary stats
create_summary_stats = SparkSubmitOperator(
    task_id='create_summary_statistics',
    application='/opt/airflow/scripts/spark_jobs/sentiment_analysis.py',
    application_args=['summary_stats', '/data/processed/master_dataset', '/data/analysis/summary'],
    conn_id='spark_default',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.executor.memory': '2g',
        'spark.driver.memory': '1g',
    },
    dag=dag,
)

# Hive: Repair table partitions
repair_hive_tables = BashOperator(
    task_id='repair_hive_table_partitions',
    bash_command="""
    beeline -u "jdbc:hive2://hive-server:10000" -e "
    MSCK REPAIR TABLE financial_lakehouse.raw_stock_prices;
    MSCK REPAIR TABLE financial_lakehouse.raw_news_sentiment;
    MSCK REPAIR TABLE financial_lakehouse.raw_sec_filings;
    MSCK REPAIR TABLE financial_lakehouse.master_dataset;
    "
    """,
    dag=dag,
)

# Success notification
pipeline_complete = BashOperator(
    task_id='pipeline_complete_notification',
    bash_command='echo "Financial Data Pipeline completed successfully on {{ ds }}"',
    dag=dag,
)

# =====================================
# Task Dependencies
# =====================================

# Extract -> Upload -> Clean
extract_group >> upload_group >> clean_group

# Merge price data after cleaning polygon and alpha vantage
[clean_group] >> merge_price_data

# Calculate indicators after merging
merge_price_data >> calculate_indicators

# Aggregate sentiment after cleaning news
[clean_group] >> aggregate_sentiment

# Create master dataset after indicators, sentiment, and SEC cleaning
[calculate_indicators, aggregate_sentiment, clean_group] >> create_master_dataset

# Analysis tasks after master dataset
create_master_dataset >> [analyze_correlation, detect_events, create_summary_stats]

# Repair Hive tables after creating master dataset
create_master_dataset >> repair_hive_tables

# Pipeline complete after all analysis
[analyze_correlation, detect_events, create_summary_stats, repair_hive_tables] >> pipeline_complete
