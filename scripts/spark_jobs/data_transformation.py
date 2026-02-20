"""
Spark Job: Data Transformation
Kết hợp và transform dữ liệu từ nhiều nguồn
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


class DataTransformationJob:
    """
    Spark job để transform và kết hợp dữ liệu
    """
    
    def __init__(self, spark: SparkSession):
        """
        Khởi tạo data transformation job
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        logger.info("Initialized DataTransformationJob")
    
    def merge_price_data(
        self,
        polygon_path: str,
        alpha_vantage_path: str,
        output_path: str
    ):
        """
        Merge dữ liệu giá từ Polygon và Alpha Vantage
        
        Args:
            polygon_path: Path to cleaned Polygon data
            alpha_vantage_path: Path to cleaned Alpha Vantage data
            output_path: Output path for merged data
        """
        logger.info("Merging price data from multiple sources")
        
        try:
            # Đọc dữ liệu
            polygon_df = self.spark.read.parquet(polygon_path)
            alpha_vantage_df = self.spark.read.parquet(alpha_vantage_path)
            
            # Select relevant columns từ Polygon
            polygon_data = polygon_df.select(
                'ticker',
                'date',
                F.col('open').alias('polygon_open'),
                F.col('high').alias('polygon_high'),
                F.col('low').alias('polygon_low'),
                F.col('close').alias('polygon_close'),
                F.col('volume').alias('polygon_volume'),
                F.col('vwap').alias('polygon_vwap'),
                'num_transactions'
            )
            
            # Select relevant columns từ Alpha Vantage
            av_data = alpha_vantage_df.select(
                'ticker',
                'date',
                F.col('open').alias('av_open'),
                F.col('high').alias('av_high'),
                F.col('low').alias('av_low'),
                F.col('close').alias('av_close'),
                F.col('volume').alias('av_volume')
            )
            
            # Full outer join để có dữ liệu từ cả hai nguồn
            merged_df = polygon_data.join(
                av_data,
                on=['ticker', 'date'],
                how='full_outer'
            )
            
            # Coalesce values - ưu tiên Polygon, fallback to Alpha Vantage
            merged_df = merged_df.select(
                'ticker',
                'date',
                F.coalesce('polygon_open', 'av_open').alias('open'),
                F.coalesce('polygon_high', 'av_high').alias('high'),
                F.coalesce('polygon_low', 'av_low').alias('low'),
                F.coalesce('polygon_close', 'av_close').alias('close'),
                F.coalesce('polygon_volume', 'av_volume').alias('volume'),
                'polygon_vwap',
                'num_transactions',
                # Keep original values for comparison
                'polygon_close',
                'av_close'
            )
            
            # Calculate price discrepancy
            merged_df = merged_df.withColumn(
                'price_discrepancy',
                F.when(
                    (F.col('polygon_close').isNotNull()) & (F.col('av_close').isNotNull()),
                    F.abs(F.col('polygon_close') - F.col('av_close'))
                ).otherwise(0.0)
            )
            
            # Thêm các derived metrics
            merged_df = merged_df.withColumn(
                'price_range',
                F.col('high') - F.col('low')
            )
            
            merged_df = merged_df.withColumn(
                'price_change',
                F.col('close') - F.col('open')
            )
            
            merged_df = merged_df.withColumn(
                'price_change_pct',
                F.when(
                    F.col('open') != 0,
                    (F.col('price_change') / F.col('open')) * 100
                ).otherwise(0.0)
            )
            
            # Add partition columns
            merged_df = merged_df.withColumn('year', F.year('date'))
            merged_df = merged_df.withColumn('month', F.month('date'))
            
            # Write to parquet
            merged_df.write.mode('overwrite').partitionBy('year', 'month').parquet(output_path)
            
            logger.info(f"Merged price data written to {output_path}")
            logger.info(f"Total records: {merged_df.count()}")
            
        except Exception as e:
            logger.error(f"Error merging price data: {str(e)}")
            raise
    
    def calculate_technical_indicators(
        self,
        price_data_path: str,
        output_path: str
    ):
        """
        Tính các technical indicators
        
        Args:
            price_data_path: Path to price data
            output_path: Output path for data with indicators
        """
        logger.info("Calculating technical indicators")
        
        try:
            df = self.spark.read.parquet(price_data_path)
            
            # Define window specs
            # Window 7 days
            window_7 = Window.partitionBy('ticker').orderBy('date').rowsBetween(-6, 0)
            # Window 20 days
            window_20 = Window.partitionBy('ticker').orderBy('date').rowsBetween(-19, 0)
            # Window 50 days
            window_50 = Window.partitionBy('ticker').orderBy('date').rowsBetween(-49, 0)
            # Window 200 days
            window_200 = Window.partitionBy('ticker').orderBy('date').rowsBetween(-199, 0)
            
            # Moving Averages
            df = df.withColumn('sma_7', F.avg('close').over(window_7))
            df = df.withColumn('sma_20', F.avg('close').over(window_20))
            df = df.withColumn('sma_50', F.avg('close').over(window_50))
            df = df.withColumn('sma_200', F.avg('close').over(window_200))
            
            # Exponential Moving Average (approximation)
            df = df.withColumn('ema_12', F.avg('close').over(window_20))
            df = df.withColumn('ema_26', F.avg('close').over(window_50))
            
            # Bollinger Bands (20-day, 2 std dev)
            df = df.withColumn('bb_middle', F.avg('close').over(window_20))
            df = df.withColumn('bb_std', F.stddev('close').over(window_20))
            df = df.withColumn('bb_upper', F.col('bb_middle') + (2 * F.col('bb_std')))
            df = df.withColumn('bb_lower', F.col('bb_middle') - (2 * F.col('bb_std')))
            
            # Volume moving average
            df = df.withColumn('volume_sma_20', F.avg('volume').over(window_20))
            
            # Price momentum
            window_lag_1 = Window.partitionBy('ticker').orderBy('date').rowsBetween(-1, -1)
            window_lag_5 = Window.partitionBy('ticker').orderBy('date').rowsBetween(-5, -5)
            window_lag_20 = Window.partitionBy('ticker').orderBy('date').rowsBetween(-20, -20)
            
            df = df.withColumn('prev_close_1', F.lag('close', 1).over(Window.partitionBy('ticker').orderBy('date')))
            df = df.withColumn('prev_close_5', F.lag('close', 5).over(Window.partitionBy('ticker').orderBy('date')))
            df = df.withColumn('prev_close_20', F.lag('close', 20).over(Window.partitionBy('ticker').orderBy('date')))
            
            df = df.withColumn(
                'momentum_1d',
                F.when(F.col('prev_close_1').isNotNull(), 
                       (F.col('close') - F.col('prev_close_1')) / F.col('prev_close_1') * 100)
                .otherwise(0.0)
            )
            
            df = df.withColumn(
                'momentum_5d',
                F.when(F.col('prev_close_5').isNotNull(),
                       (F.col('close') - F.col('prev_close_5')) / F.col('prev_close_5') * 100)
                .otherwise(0.0)
            )
            
            df = df.withColumn(
                'momentum_20d',
                F.when(F.col('prev_close_20').isNotNull(),
                       (F.col('close') - F.col('prev_close_20')) / F.col('prev_close_20') * 100)
                .otherwise(0.0)
            )
            
            # Volatility (20-day standard deviation of returns)
            df = df.withColumn('volatility_20d', F.stddev('price_change_pct').over(window_20))
            
            # Relative Volume
            df = df.withColumn(
                'relative_volume',
                F.when(F.col('volume_sma_20') > 0,
                       F.col('volume') / F.col('volume_sma_20'))
                .otherwise(1.0)
            )
            
            # Drop intermediate columns
            df = df.drop('prev_close_1', 'prev_close_5', 'prev_close_20', 'bb_std')
            
            # Write to parquet
            df.write.mode('overwrite').partitionBy('year', 'month').parquet(output_path)
            
            logger.info(f"Technical indicators calculated and written to {output_path}")
            logger.info(f"Total records: {df.count()}")
            
        except Exception as e:
            logger.error(f"Error calculating technical indicators: {str(e)}")
            raise
    
    def aggregate_daily_sentiment(
        self,
        news_sentiment_path: str,
        output_path: str
    ):
        """
        Aggregate sentiment scores by ticker and date
        
        Args:
            news_sentiment_path: Path to news sentiment data
            output_path: Output path for aggregated sentiment
        """
        logger.info("Aggregating daily sentiment scores")
        
        try:
            df = self.spark.read.parquet(news_sentiment_path)
            
            # Aggregate by ticker and date
            agg_df = df.groupBy('ticker', 'date').agg(
                F.count('*').alias('news_count'),
                F.avg('sentiment_score_normalized').alias('avg_sentiment'),
                F.min('sentiment_score_normalized').alias('min_sentiment'),
                F.max('sentiment_score_normalized').alias('max_sentiment'),
                F.stddev('sentiment_score_normalized').alias('sentiment_stddev'),
                # Count by sentiment label
                F.sum(F.when(F.col('sentiment_label') == 'Bullish', 1).otherwise(0)).alias('bullish_count'),
                F.sum(F.when(F.col('sentiment_label') == 'Bearish', 1).otherwise(0)).alias('bearish_count'),
                F.sum(F.when(F.col('sentiment_label') == 'Neutral', 1).otherwise(0)).alias('neutral_count')
            )
            
            # Calculate sentiment ratios
            agg_df = agg_df.withColumn(
                'bullish_ratio',
                F.when(F.col('news_count') > 0, F.col('bullish_count') / F.col('news_count')).otherwise(0.0)
            )
            
            agg_df = agg_df.withColumn(
                'bearish_ratio',
                F.when(F.col('news_count') > 0, F.col('bearish_count') / F.col('news_count')).otherwise(0.0)
            )
            
            # Weighted sentiment (more weight to recent news)
            df_with_recency = df.withColumn(
                'hours_ago',
                (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp('published_at')) / 3600
            )
            
            df_with_recency = df_with_recency.withColumn(
                'recency_weight',
                F.exp(-F.col('hours_ago') / 24.0)  # Exponential decay, half-life = 1 day
            )
            
            weighted_sentiment = df_with_recency.groupBy('ticker', 'date').agg(
                F.sum(F.col('sentiment_score_normalized') * F.col('recency_weight')).alias('weighted_sentiment_sum'),
                F.sum('recency_weight').alias('total_weight')
            )
            
            weighted_sentiment = weighted_sentiment.withColumn(
                'weighted_avg_sentiment',
                F.when(F.col('total_weight') > 0, F.col('weighted_sentiment_sum') / F.col('total_weight'))
                .otherwise(0.0)
            )
            
            # Join with aggregated data
            final_df = agg_df.join(
                weighted_sentiment.select('ticker', 'date', 'weighted_avg_sentiment'),
                on=['ticker', 'date'],
                how='left'
            )
            
            # Add partition columns
            final_df = final_df.withColumn('year', F.year('date'))
            final_df = final_df.withColumn('month', F.month('date'))
            
            # Write to parquet
            final_df.write.mode('overwrite').partitionBy('year', 'month').parquet(output_path)
            
            logger.info(f"Aggregated sentiment data written to {output_path}")
            logger.info(f"Total records: {final_df.count()}")
            
        except Exception as e:
            logger.error(f"Error aggregating sentiment: {str(e)}")
            raise
    
    def create_master_dataset(
        self,
        price_indicators_path: str,
        sentiment_path: str,
        sec_path: str,
        output_path: str
    ):
        """
        Tạo master dataset kết hợp tất cả dữ liệu
        
        Args:
            price_indicators_path: Path to price data with indicators
            sentiment_path: Path to aggregated sentiment data
            sec_path: Path to SEC filing data
            output_path: Output path for master dataset
        """
        logger.info("Creating master dataset")
        
        try:
            # Đọc dữ liệu
            price_df = self.spark.read.parquet(price_indicators_path)
            sentiment_df = self.spark.read.parquet(sentiment_path)
            sec_df = self.spark.read.parquet(sec_path)
            
            # Aggregate SEC filings by ticker and date
            sec_agg = sec_df.groupBy('ticker', F.to_date('filing_date').alias('date')).agg(
                F.count('*').alias('filing_count'),
                F.collect_set('form_type').alias('form_types')
            )
            
            # Join price với sentiment
            master_df = price_df.join(
                sentiment_df,
                on=['ticker', 'date'],
                how='left'
            )
            
            # Join với SEC data
            master_df = master_df.join(
                sec_agg,
                on=['ticker', 'date'],
                how='left'
            )
            
            # Fill nulls cho sentiment columns
            sentiment_cols = [
                'news_count', 'avg_sentiment', 'min_sentiment', 'max_sentiment',
                'sentiment_stddev', 'bullish_count', 'bearish_count', 'neutral_count',
                'bullish_ratio', 'bearish_ratio', 'weighted_avg_sentiment'
            ]
            
            for col in sentiment_cols:
                if col in master_df.columns:
                    master_df = master_df.withColumn(col, F.coalesce(F.col(col), F.lit(0.0)))
            
            # Fill nulls cho SEC columns
            master_df = master_df.withColumn('filing_count', F.coalesce(F.col('filing_count'), F.lit(0)))
            
            # Add day of week (0 = Monday, 6 = Sunday)
            master_df = master_df.withColumn('day_of_week', F.dayofweek('date'))
            
            # Add is_filing_date flag
            master_df = master_df.withColumn(
                'is_filing_date',
                F.when(F.col('filing_count') > 0, 1).otherwise(0)
            )
            
            # Write to parquet
            master_df.write.mode('overwrite').partitionBy('year', 'month').parquet(output_path)
            
            logger.info(f"Master dataset created and written to {output_path}")
            logger.info(f"Total records: {master_df.count()}")
            
        except Exception as e:
            logger.error(f"Error creating master dataset: {str(e)}")
            raise


def main():
    """
    Main function để chạy data transformation job
    """
    if len(sys.argv) < 2:
        print("Usage: data_transformation.py <job_type> [args...]")
        print("job_types:")
        print("  merge_price <polygon_path> <av_path> <output_path>")
        print("  calc_indicators <price_path> <output_path>")
        print("  agg_sentiment <sentiment_path> <output_path>")
        print("  create_master <price_path> <sentiment_path> <sec_path> <output_path>")
        sys.exit(1)
    
    job_type = sys.argv[1]
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Financial-DataTransformation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    job = DataTransformationJob(spark)
    
    if job_type == 'merge_price' and len(sys.argv) == 5:
        job.merge_price_data(sys.argv[2], sys.argv[3], sys.argv[4])
    elif job_type == 'calc_indicators' and len(sys.argv) == 4:
        job.calculate_technical_indicators(sys.argv[2], sys.argv[3])
    elif job_type == 'agg_sentiment' and len(sys.argv) == 4:
        job.aggregate_daily_sentiment(sys.argv[2], sys.argv[3])
    elif job_type == 'create_master' and len(sys.argv) == 6:
        job.create_master_dataset(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    else:
        logger.error(f"Invalid job type or arguments")
        sys.exit(1)
    
    spark.stop()
    logger.info("Data transformation job completed successfully")


if __name__ == '__main__':
    main()
