"""
Spark Job: Data Cleaning
Làm sạch và chuẩn hóa dữ liệu từ HDFS
"""

import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataCleaningJob:
    """
    Spark job để làm sạch dữ liệu tài chính
    """
    
    def __init__(self, spark: SparkSession):
        """
        Khởi tạo data cleaning job
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        logger.info("Initialized DataCleaningJob")
    
    def clean_polygon_data(self, input_path: str, output_path: str):
        """
        Làm sạch dữ liệu từ Polygon API
        
        Args:
            input_path: HDFS path to raw Polygon data
            output_path: HDFS path for cleaned data
        """
        logger.info(f"Cleaning Polygon data from {input_path}")
        
        try:
            # Đọc JSON data
            df = self.spark.read.json(input_path)
            
            # Explode nested results array
            if 'aggregates' in df.columns:
                df = df.select('ticker', 'extracted_at', F.explode('aggregates.results').alias('data'))
                df = df.select(
                    'ticker',
                    'extracted_at',
                    F.col('data.t').alias('timestamp'),
                    F.col('data.o').alias('open'),
                    F.col('data.h').alias('high'),
                    F.col('data.l').alias('low'),
                    F.col('data.c').alias('close'),
                    F.col('data.v').alias('volume'),
                    F.col('data.vw').alias('vwap'),
                    F.col('data.n').alias('num_transactions')
                )
            
            # Convert timestamp từ milliseconds sang date
            df = df.withColumn(
                'date',
                F.to_date(F.from_unixtime(F.col('timestamp') / 1000))
            )
            
            # Loại bỏ duplicates
            window_spec = Window.partitionBy('ticker', 'date').orderBy(F.col('extracted_at').desc())
            df = df.withColumn('rn', F.row_number().over(window_spec))
            df = df.filter(F.col('rn') == 1).drop('rn')
            
            # Loại bỏ null values trong các cột quan trọng
            df = df.filter(
                F.col('open').isNotNull() &
                F.col('high').isNotNull() &
                F.col('low').isNotNull() &
                F.col('close').isNotNull() &
                F.col('volume').isNotNull()
            )
            
            # Data validation: high >= low, high >= open, high >= close
            df = df.filter(
                (F.col('high') >= F.col('low')) &
                (F.col('high') >= F.col('open')) &
                (F.col('high') >= F.col('close')) &
                (F.col('low') <= F.col('open')) &
                (F.col('low') <= F.col('close'))
            )
            
            # Thêm các derived columns
            df = df.withColumn('price_range', F.col('high') - F.col('low'))
            df = df.withColumn('price_change', F.col('close') - F.col('open'))
            df = df.withColumn(
                'price_change_pct',
                F.when(F.col('open') != 0, (F.col('price_change') / F.col('open')) * 100).otherwise(0)
            )
            
            # Thêm partition columns
            df = df.withColumn('year', F.year('date'))
            df = df.withColumn('month', F.month('date'))
            df = df.withColumn('day', F.dayofmonth('date'))
            
            # Select final columns
            df = df.select(
                'ticker',
                'date',
                'open',
                'high',
                'low',
                'close',
                'volume',
                'vwap',
                'num_transactions',
                'price_range',
                'price_change',
                'price_change_pct',
                'timestamp',
                'extracted_at',
                'year',
                'month',
                'day'
            )
            
            # Write to parquet với partitioning
            df.write.mode('overwrite').partitionBy('year', 'month', 'day').parquet(output_path)
            
            logger.info(f"Cleaned Polygon data written to {output_path}")
            logger.info(f"Total records: {df.count()}")
            
        except Exception as e:
            logger.error(f"Error cleaning Polygon data: {str(e)}")
            raise
    
    def clean_alpha_vantage_data(self, input_path: str, output_path: str):
        """
        Làm sạch dữ liệu từ Alpha Vantage API
        
        Args:
            input_path: HDFS path to raw Alpha Vantage data
            output_path: HDFS path for cleaned data
        """
        logger.info(f"Cleaning Alpha Vantage data from {input_path}")
        
        try:
            # Đọc JSON data
            df = self.spark.read.json(input_path)
            
            # Extract time series data
            if 'time_series' in df.columns:
                # Explode time series map
                df = df.select(
                    'symbol',
                    'extracted_at',
                    F.explode('time_series.time_series').alias('date', 'data')
                )
                
                df = df.select(
                    F.col('symbol').alias('ticker'),
                    'date',
                    F.col('data.1. open').cast(DoubleType()).alias('open'),
                    F.col('data.2. high').cast(DoubleType()).alias('high'),
                    F.col('data.3. low').cast(DoubleType()).alias('low'),
                    F.col('data.4. close').cast(DoubleType()).alias('close'),
                    F.col('data.5. volume').cast(LongType()).alias('volume'),
                    'extracted_at'
                )
            
            # Convert date string to date type
            df = df.withColumn('date', F.to_date('date'))
            
            # Loại bỏ duplicates
            window_spec = Window.partitionBy('ticker', 'date').orderBy(F.col('extracted_at').desc())
            df = df.withColumn('rn', F.row_number().over(window_spec))
            df = df.filter(F.col('rn') == 1).drop('rn')
            
            # Loại bỏ null values
            df = df.filter(
                F.col('open').isNotNull() &
                F.col('high').isNotNull() &
                F.col('low').isNotNull() &
                F.col('close').isNotNull()
            )
            
            # Data validation
            df = df.filter(
                (F.col('high') >= F.col('low')) &
                (F.col('high') >= F.col('open')) &
                (F.col('high') >= F.col('close'))
            )
            
            # Derived columns
            df = df.withColumn('price_range', F.col('high') - F.col('low'))
            df = df.withColumn('price_change', F.col('close') - F.col('open'))
            df = df.withColumn(
                'price_change_pct',
                F.when(F.col('open') != 0, (F.col('price_change') / F.col('open')) * 100).otherwise(0)
            )
            
            # Partition columns
            df = df.withColumn('year', F.year('date'))
            df = df.withColumn('month', F.month('date'))
            
            # Write to parquet
            df.write.mode('overwrite').partitionBy('year', 'month').parquet(output_path)
            
            logger.info(f"Cleaned Alpha Vantage data written to {output_path}")
            logger.info(f"Total records: {df.count()}")
            
        except Exception as e:
            logger.error(f"Error cleaning Alpha Vantage data: {str(e)}")
            raise
    
    def clean_sec_data(self, input_path: str, output_path: str):
        """
        Làm sạch dữ liệu từ SEC API
        
        Args:
            input_path: HDFS path to raw SEC data
            output_path: HDFS path for cleaned data
        """
        logger.info(f"Cleaning SEC data from {input_path}")
        
        try:
            # Đọc JSON data
            df = self.spark.read.json(input_path)
            
            # Extract recent filings
            if 'recent_filings' in df.columns:
                df = df.select(
                    'ticker',
                    'cik',
                    'extracted_at',
                    F.explode('recent_filings').alias('filing')
                )
                
                df = df.select(
                    'ticker',
                    'cik',
                    F.col('filing.accessionNumber').alias('accession_number'),
                    F.to_date(F.col('filing.filingDate')).alias('filing_date'),
                    F.to_date(F.col('filing.reportDate')).alias('report_date'),
                    F.col('filing.form').alias('form_type'),
                    F.col('filing.primaryDocument').alias('primary_document'),
                    F.col('filing.primaryDocDescription').alias('document_description'),
                    'extracted_at'
                )
            
            # Loại bỏ duplicates
            df = df.dropDuplicates(['ticker', 'accession_number'])
            
            # Loại bỏ null values trong cột quan trọng
            df = df.filter(
                F.col('ticker').isNotNull() &
                F.col('cik').isNotNull() &
                F.col('filing_date').isNotNull() &
                F.col('form_type').isNotNull()
            )
            
            # Partition columns
            df = df.withColumn('year', F.year('filing_date'))
            df = df.withColumn('month', F.month('filing_date'))
            
            # Write to parquet
            df.write.mode('overwrite').partitionBy('year', 'month').parquet(output_path)
            
            logger.info(f"Cleaned SEC data written to {output_path}")
            logger.info(f"Total records: {df.count()}")
            
        except Exception as e:
            logger.error(f"Error cleaning SEC data: {str(e)}")
            raise
    
    def clean_news_sentiment_data(self, input_path: str, output_path: str):
        """
        Làm sạch dữ liệu news sentiment từ Alpha Vantage
        
        Args:
            input_path: HDFS path to raw news data
            output_path: HDFS path for cleaned data
        """
        logger.info(f"Cleaning news sentiment data from {input_path}")
        
        try:
            # Đọc JSON data
            df = self.spark.read.json(input_path)
            
            # Extract news feed
            if 'news_sentiment' in df.columns:
                df = df.select(
                    'symbol',
                    'extracted_at',
                    F.explode('news_sentiment.feed').alias('article')
                )
                
                df = df.select(
                    F.col('symbol').alias('ticker'),
                    F.col('article.title').alias('title'),
                    F.col('article.url').alias('url'),
                    F.to_timestamp(F.col('article.time_published'), 'yyyyMMdd\'T\'HHmmss').alias('published_at'),
                    F.col('article.summary').alias('summary'),
                    F.col('article.overall_sentiment_score').cast(DoubleType()).alias('sentiment_score'),
                    F.col('article.overall_sentiment_label').alias('sentiment_label'),
                    F.col('article.source').alias('source'),
                    'extracted_at'
                )
            
            # Loại bỏ duplicates
            df = df.dropDuplicates(['ticker', 'url'])
            
            # Filter null values
            df = df.filter(
                F.col('title').isNotNull() &
                F.col('published_at').isNotNull() &
                F.col('sentiment_score').isNotNull()
            )
            
            # Normalize sentiment score (-1 to 1)
            df = df.withColumn(
                'sentiment_score_normalized',
                F.when(F.col('sentiment_score') > 1, 1.0)
                 .when(F.col('sentiment_score') < -1, -1.0)
                 .otherwise(F.col('sentiment_score'))
            )
            
            # Calculate date from published_at
            df = df.withColumn('date', F.to_date('published_at'))
            df = df.withColumn('year', F.year('date'))
            df = df.withColumn('month', F.month('date'))
            
            # Write to parquet
            df.write.mode('overwrite').partitionBy('year', 'month').parquet(output_path)
            
            logger.info(f"Cleaned news sentiment data written to {output_path}")
            logger.info(f"Total records: {df.count()}")
            
        except Exception as e:
            logger.error(f"Error cleaning news sentiment data: {str(e)}")
            raise


def main():
    """
    Main function để chạy data cleaning job
    """
    if len(sys.argv) < 4:
        print("Usage: data_cleaning.py <data_source> <input_path> <output_path>")
        print("data_source: polygon | alpha_vantage | sec | news_sentiment")
        sys.exit(1)
    
    data_source = sys.argv[1]
    input_path = sys.argv[2]
    output_path = sys.argv[3]
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Financial-DataCleaning") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Create job instance
    job = DataCleaningJob(spark)
    
    # Run appropriate cleaning method
    if data_source == 'polygon':
        job.clean_polygon_data(input_path, output_path)
    elif data_source == 'alpha_vantage':
        job.clean_alpha_vantage_data(input_path, output_path)
    elif data_source == 'sec':
        job.clean_sec_data(input_path, output_path)
    elif data_source == 'news_sentiment':
        job.clean_news_sentiment_data(input_path, output_path)
    else:
        logger.error(f"Unknown data source: {data_source}")
        sys.exit(1)
    
    spark.stop()
    logger.info("Data cleaning job completed successfully")


if __name__ == '__main__':
    main()
