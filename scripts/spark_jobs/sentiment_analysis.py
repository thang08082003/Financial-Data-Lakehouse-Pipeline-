"""
Spark Job: Sentiment Analysis  
Phân tích mối tương quan giữa sentiment và biến động giá
"""

import sys
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SentimentAnalysisJob:
    """
    Spark job để phân tích sentiment và correlation với giá
    """
    
    def __init__(self, spark: SparkSession):
        """
        Khởi tạo sentiment analysis job
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        logger.info("Initialized SentimentAnalysisJob")
    
    def analyze_sentiment_price_correlation(
        self,
        master_data_path: str,
        output_path: str
    ):
        """
        Phân tích correlation giữa sentiment và price movements
        
        Args:
            master_data_path: Path to master dataset
            output_path: Output path for analysis results
        """
        logger.info("Analyzing sentiment-price correlation")
        
        try:
            df = self.spark.read.parquet(master_data_path)
            
            # Filter records có sentiment data
            df_with_sentiment = df.filter(F.col('news_count') > 0)
            
            # Calculate next day price change
            window_spec = Window.partitionBy('ticker').orderBy('date')
            df_with_sentiment = df_with_sentiment.withColumn(
                'next_day_close',
                F.lead('close', 1).over(window_spec)
            )
            
            df_with_sentiment = df_with_sentiment.withColumn(
                'next_day_price_change',
                F.when(
                    (F.col('next_day_close').isNotNull()) & (F.col('close') != 0),
                    (F.col('next_day_close') - F.col('close')) / F.col('close') * 100
                ).otherwise(None)
            )
            
            # Filter out nulls
            df_analysis = df_with_sentiment.filter(F.col('next_day_price_change').isNotNull())
            
            # Calculate correlation by ticker
            correlation_by_ticker = df_analysis.groupBy('ticker').agg(
                F.count('*').alias('sample_size'),
                F.corr('avg_sentiment', 'next_day_price_change').alias('sentiment_correlation'),
                F.corr('weighted_avg_sentiment', 'next_day_price_change').alias('weighted_sentiment_correlation'),
                F.corr('bullish_ratio', 'next_day_price_change').alias('bullish_ratio_correlation'),
                F.corr('news_count', 'next_day_price_change').alias('news_volume_correlation'),
                F.avg('avg_sentiment').alias('avg_sentiment'),
                F.avg('next_day_price_change').alias('avg_next_day_change')
            )
            
            # Calculate overall correlation
            overall_correlation = df_analysis.agg(
                F.count('*').alias('total_samples'),
                F.corr('avg_sentiment', 'next_day_price_change').alias('overall_sentiment_correlation'),
                F.corr('weighted_avg_sentiment', 'next_day_price_change').alias('overall_weighted_correlation'),
                F.corr('bullish_ratio', 'next_day_price_change').alias('overall_bullish_correlation')
            )
            
            # Add metadata
            correlation_by_ticker = correlation_by_ticker.withColumn(
                'analysis_date',
                F.lit(datetime.now().isoformat())
            )
            
            overall_correlation = overall_correlation.withColumn(
                'analysis_date',
                F.lit(datetime.now().isoformat())
            )
            
            # Write results
            correlation_by_ticker.write.mode('overwrite').parquet(f"{output_path}/by_ticker")
            overall_correlation.write.mode('overwrite').parquet(f"{output_path}/overall")
            
            logger.info(f"Correlation analysis completed and written to {output_path}")
            
            # Log some statistics
            overall_stats = overall_correlation.collect()[0]
            logger.info(f"Overall sentiment correlation: {overall_stats['overall_sentiment_correlation']:.4f}")
            logger.info(f"Total samples: {overall_stats['total_samples']}")
            
        except Exception as e:
            logger.error(f"Error in correlation analysis: {str(e)}")
            raise
    
    def analyze_sentiment_by_timeframe(
        self,
        master_data_path: str,
        output_path: str
    ):
        """
        Phân tích sentiment impact trên các timeframes khác nhau
        
        Args:
            master_data_path: Path to master dataset
            output_path: Output path for timeframe analysis
        """
        logger.info("Analyzing sentiment by timeframe")
        
        try:
            df = self.spark.read.parquet(master_data_path)
            df = df.filter(F.col('news_count') > 0)
            
            window_spec = Window.partitionBy('ticker').orderBy('date')
            
            # Calculate price changes for different timeframes
            for days in [1, 3, 5, 10, 20]:
                df = df.withColumn(
                    f'close_{days}d_later',
                    F.lead('close', days).over(window_spec)
                )
                
                df = df.withColumn(
                    f'price_change_{days}d',
                    F.when(
                        (F.col(f'close_{days}d_later').isNotNull()) & (F.col('close') != 0),
                        (F.col(f'close_{days}d_later') - F.col('close')) / F.col('close') * 100
                    ).otherwise(None)
                )
            
            # Group by sentiment ranges
            df = df.withColumn(
                'sentiment_category',
                F.when(F.col('avg_sentiment') < -0.3, 'Very Bearish')
                 .when((F.col('avg_sentiment') >= -0.3) & (F.col('avg_sentiment') < -0.1), 'Bearish')
                 .when((F.col('avg_sentiment') >= -0.1) & (F.col('avg_sentiment') <= 0.1), 'Neutral')
                 .when((F.col('avg_sentiment') > 0.1) & (F.col('avg_sentiment') <= 0.3), 'Bullish')
                 .otherwise('Very Bullish')
            )
            
            # Aggregate by sentiment category
            timeframe_analysis = df.groupBy('sentiment_category').agg(
                F.count('*').alias('count'),
                F.avg('price_change_1d').alias('avg_1d_change'),
                F.avg('price_change_3d').alias('avg_3d_change'),
                F.avg('price_change_5d').alias('avg_5d_change'),
                F.avg('price_change_10d').alias('avg_10d_change'),
                F.avg('price_change_20d').alias('avg_20d_change'),
                F.stddev('price_change_1d').alias('stddev_1d_change'),
                F.stddev('price_change_5d').alias('stddev_5d_change')
            )
            
            # Order by sentiment
            sentiment_order = F.when(F.col('sentiment_category') == 'Very Bearish', 1) \
                .when(F.col('sentiment_category') == 'Bearish', 2) \
                .when(F.col('sentiment_category') == 'Neutral', 3) \
                .when(F.col('sentiment_category') == 'Bullish', 4) \
                .otherwise(5)
            
            timeframe_analysis = timeframe_analysis.withColumn('order', sentiment_order)
            timeframe_analysis = timeframe_analysis.orderBy('order').drop('order')
            
            # Write results
            timeframe_analysis.write.mode('overwrite').parquet(output_path)
            
            logger.info(f"Timeframe analysis completed and written to {output_path}")
            
        except Exception as e:
            logger.error(f"Error in timeframe analysis: {str(e)}")
            raise
    
    def detect_sentiment_driven_events(
        self,
        master_data_path: str,
        output_path: str,
        min_news_count: int = 10,
        sentiment_threshold: float = 0.5
    ):
        """
        Phát hiện các sự kiện có sentiment mạnh và impact lên giá
        
        Args:
            master_data_path: Path to master dataset
            output_path: Output path for detected events
            min_news_count: Minimum news count để được coi là event
            sentiment_threshold: Minimum abs(sentiment) để được coi là strong sentiment
        """
        logger.info("Detecting sentiment-driven events")
        
        try:
            df = self.spark.read.parquet(master_data_path)
            
            # Filter high news volume days với strong sentiment
            events_df = df.filter(
                (F.col('news_count') >= min_news_count) &
                (F.abs(F.col('avg_sentiment')) >= sentiment_threshold)
            )
            
            # Calculate next day impact
            window_spec = Window.partitionBy('ticker').orderBy('date')
            events_df = events_df.withColumn(
                'next_day_close',
                F.lead('close', 1).over(window_spec)
            )
            
            events_df = events_df.withColumn(
                'next_day_price_change',
                F.when(
                    (F.col('next_day_close').isNotNull()) & (F.col('close') != 0),
                    (F.col('next_day_close') - F.col('close')) / F.col('close') * 100
                ).otherwise(None)
            )
            
            # Calculate intraday volatility
            events_df = events_df.withColumn(
                'intraday_volatility',
                F.when(F.col('close') != 0, (F.col('high') - F.col('low')) / F.col('close') * 100)
                .otherwise(0.0)
            )
            
            # Select relevant columns
            events_df = events_df.select(
                'ticker',
                'date',
                'close',
                'news_count',
                'avg_sentiment',
                'weighted_avg_sentiment',
                'bullish_ratio',
                'bearish_ratio',
                'bullish_count',
                'bearish_count',
                'neutral_count',
                'price_change_pct',
                'next_day_price_change',
                'intraday_volatility',
                'volume',
                'relative_volume',
                'filing_count',
                'form_types'
            )
            
            # Add event classification
            events_df = events_df.withColumn(
                'event_type',
                F.when(F.col('avg_sentiment') >= sentiment_threshold, 'Positive Sentiment Event')
                 .when(F.col('avg_sentiment') <= -sentiment_threshold, 'Negative Sentiment Event')
                 .otherwise('Mixed Sentiment Event')
            )
            
            # Calculate alignment score (sentiment vs actual price movement)
            events_df = events_df.withColumn(
                'sentiment_price_alignment',
                F.when(
                    (F.col('next_day_price_change').isNotNull()) & (F.col('avg_sentiment') != 0),
                    F.signum(F.col('avg_sentiment')) * F.signum(F.col('next_day_price_change'))
                ).otherwise(0)
            )
            
            # Order by date descending
            events_df = events_df.orderBy(F.col('date').desc())
            
            # Write results
            events_df.write.mode('overwrite').partitionBy('ticker').parquet(output_path)
            
            logger.info(f"Detected {events_df.count()} sentiment-driven events")
            logger.info(f"Events written to {output_path}")
            
        except Exception as e:
            logger.error(f"Error detecting sentiment events: {str(e)}")
            raise
    
    def create_sentiment_summary_stats(
        self,
        master_data_path: str,
        output_path: str
    ):
        """
        Tạo summary statistics cho sentiment analysis
        
        Args:
            master_data_path: Path to master dataset
            output_path: Output path for summary stats
        """
        logger.info("Creating sentiment summary statistics")
        
        try:
            df = self.spark.read.parquet(master_data_path)
            df_with_news = df.filter(F.col('news_count') > 0)
            
            # Overall statistics
            overall_stats = df_with_news.agg(
                F.count('*').alias('total_days_with_news'),
                F.sum('news_count').alias('total_news_articles'),
                F.avg('news_count').alias('avg_news_per_day'),
                F.avg('avg_sentiment').alias('overall_avg_sentiment'),
                F.stddev('avg_sentiment').alias('sentiment_stddev'),
                F.min('avg_sentiment').alias('min_sentiment'),
                F.max('avg_sentiment').alias('max_sentiment'),
                F.avg('bullish_ratio').alias('avg_bullish_ratio'),
                F.avg('bearish_ratio').alias('avg_bearish_ratio')
            )
            
            # Statistics by ticker
            ticker_stats = df_with_news.groupBy('ticker').agg(
                F.count('*').alias('days_with_news'),
                F.sum('news_count').alias('total_news'),
                F.avg('news_count').alias('avg_news_per_day'),
                F.avg('avg_sentiment').alias('avg_sentiment'),
                F.stddev('avg_sentiment').alias('sentiment_volatility'),
                F.avg('bullish_ratio').alias('avg_bullish_ratio'),
                F.avg('bearish_ratio').alias('avg_bearish_ratio'),
                F.avg('price_change_pct').alias('avg_price_change'),
                F.corr('avg_sentiment', 'price_change_pct').alias('same_day_correlation')
            )
            
            # Monthly trends
            monthly_trends = df_with_news.groupBy(
                'year',
                'month'
            ).agg(
                F.count('*').alias('trading_days'),
                F.sum('news_count').alias('total_news'),
                F.avg('avg_sentiment').alias('avg_sentiment'),
                F.avg('bullish_ratio').alias('avg_bullish_ratio'),
                F.avg('price_change_pct').alias('avg_price_change')
            ).orderBy('year', 'month')
            
            # Add analysis metadata
            analysis_timestamp = datetime.now().isoformat()
            overall_stats = overall_stats.withColumn('generated_at', F.lit(analysis_timestamp))
            ticker_stats = ticker_stats.withColumn('generated_at', F.lit(analysis_timestamp))
            monthly_trends = monthly_trends.withColumn('generated_at', F.lit(analysis_timestamp))
            
            # Write results
            overall_stats.write.mode('overwrite').parquet(f"{output_path}/overall")
            ticker_stats.write.mode('overwrite').parquet(f"{output_path}/by_ticker")
            monthly_trends.write.mode('overwrite').parquet(f"{output_path}/monthly_trends")
            
            logger.info(f"Summary statistics written to {output_path}")
            
        except Exception as e:
            logger.error(f"Error creating summary stats: {str(e)}")
            raise


def main():
    """
    Main function để chạy sentiment analysis job
    """
    if len(sys.argv) < 2:
        print("Usage: sentiment_analysis.py <job_type> [args...]")
        print("job_types:")
        print("  correlation <master_data_path> <output_path>")
        print("  timeframe <master_data_path> <output_path>")
        print("  detect_events <master_data_path> <output_path> [min_news] [sentiment_threshold]")
        print("  summary_stats <master_data_path> <output_path>")
        sys.exit(1)
    
    job_type = sys.argv[1]
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Financial-SentimentAnalysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    job = SentimentAnalysisJob(spark)
    
    if job_type == 'correlation' and len(sys.argv) == 4:
        job.analyze_sentiment_price_correlation(sys.argv[2], sys.argv[3])
    elif job_type == 'timeframe' and len(sys.argv) == 4:
        job.analyze_sentiment_by_timeframe(sys.argv[2], sys.argv[3])
    elif job_type == 'detect_events':
        min_news = int(sys.argv[4]) if len(sys.argv) > 4 else 10
        threshold = float(sys.argv[5]) if len(sys.argv) > 5 else 0.5
        job.detect_sentiment_driven_events(sys.argv[2], sys.argv[3], min_news, threshold)
    elif job_type == 'summary_stats' and len(sys.argv) == 4:
        job.create_sentiment_summary_stats(sys.argv[2], sys.argv[3])
    else:
        logger.error("Invalid job type or arguments")
        sys.exit(1)
    
    spark.stop()
    logger.info("Sentiment analysis job completed successfully")


if __name__ == '__main__':
    main()
