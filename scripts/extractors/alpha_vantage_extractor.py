"""
Alpha Vantage API Extractor
Thu thập dữ liệu tài chính và tin tức từ Alpha Vantage API
"""

import os
import json
import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AlphaVantageExtractor:
    """
    Extractor để lấy dữ liệu từ Alpha Vantage API
    """
    
    BASE_URL = "https://www.alphavantage.co/query"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Khởi tạo Alpha Vantage extractor
        
        Args:
            api_key: API key của Alpha Vantage
        """
        self.api_key = api_key or os.getenv('ALPHA_VANTAGE_API_KEY')
        if not self.api_key:
            raise ValueError("ALPHA_VANTAGE_API_KEY không được cung cấp")
        
        self.session = requests.Session()
        # Alpha Vantage free tier: 5 API calls/minute, 500 calls/day
        self.rate_limit_delay = 12  # seconds between calls
    
    def _make_request(self, params: Dict) -> Dict:
        """
        Thực hiện API request với rate limiting
        
        Args:
            params: Parameters cho API request
            
        Returns:
            API response as dictionary
        """
        params['apikey'] = self.api_key
        
        try:
            response = self.session.get(self.BASE_URL, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                raise ValueError(f"API Error: {data['Error Message']}")
            if 'Note' in data:
                logger.warning(f"API Note: {data['Note']}")
                time.sleep(60)  # Wait 1 minute if rate limited
                return self._make_request(params)
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
            
            return data
            
        except requests.RequestException as e:
            logger.error(f"Request error: {str(e)}")
            raise
    
    def get_time_series_daily(
        self,
        symbol: str,
        outputsize: str = 'full'
    ) -> Dict:
        """
        Lấy dữ liệu time series daily (OHLCV)
        
        Args:
            symbol: Mã cổ phiếu
            outputsize: 'compact' (100 data points) hoặc 'full' (20+ years)
            
        Returns:
            Dictionary chứa dữ liệu time series
        """
        logger.info(f"Fetching daily time series for {symbol}")
        
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'outputsize': outputsize
        }
        
        data = self._make_request(params)
        
        return {
            'symbol': symbol,
            'metadata': data.get('Meta Data', {}),
            'time_series': data.get('Time Series (Daily)', {}),
            'extracted_at': datetime.utcnow().isoformat()
        }
    
    def get_company_overview(self, symbol: str) -> Dict:
        """
        Lấy thông tin tổng quan về công ty
        
        Args:
            symbol: Mã cổ phiếu
            
        Returns:
            Dictionary chứa thông tin công ty
        """
        logger.info(f"Fetching company overview for {symbol}")
        
        params = {
            'function': 'OVERVIEW',
            'symbol': symbol
        }
        
        data = self._make_request(params)
        
        return {
            'symbol': symbol,
            'overview': data,
            'extracted_at': datetime.utcnow().isoformat()
        }
    
    def get_news_sentiment(
        self,
        tickers: Optional[str] = None,
        topics: Optional[str] = None,
        time_from: Optional[str] = None,
        time_to: Optional[str] = None,
        limit: int = 50
    ) -> Dict:
        """
        Lấy dữ liệu tin tức và sentiment analysis
        
        Args:
            tickers: Danh sách mã cổ phiếu (phân cách bởi dấu phẩy)
            topics: Topics (blockchain, earnings, ipo, etc.)
            time_from: Thời gian bắt đầu (YYYYMMDDThhmm)
            time_to: Thời gian kết thúc (YYYYMMDDThhmm)
            limit: Số lượng bài viết tối đa (1-1000)
            
        Returns:
            Dictionary chứa dữ liệu news sentiment
        """
        logger.info(f"Fetching news sentiment for {tickers or 'all tickers'}")
        
        params = {
            'function': 'NEWS_SENTIMENT',
            'limit': limit
        }
        
        if tickers:
            params['tickers'] = tickers
        if topics:
            params['topics'] = topics
        if time_from:
            params['time_from'] = time_from
        if time_to:
            params['time_to'] = time_to
        
        data = self._make_request(params)
        
        return {
            'tickers': tickers,
            'items': data.get('items', str(data.get('items', 0))),
            'sentiment_score_definition': data.get('sentiment_score_definition', ''),
            'relevance_score_definition': data.get('relevance_score_definition', ''),
            'feed': data.get('feed', []),
            'extracted_at': datetime.utcnow().isoformat()
        }
    
    def get_technical_indicator(
        self,
        symbol: str,
        function: str = 'RSI',
        interval: str = 'daily',
        time_period: int = 14,
        series_type: str = 'close'
    ) -> Dict:
        """
        Lấy technical indicators (RSI, MACD, SMA, EMA, etc.)
        
        Args:
            symbol: Mã cổ phiếu
            function: Tên indicator (RSI, MACD, SMA, EMA, STOCH, ADX, etc.)
            interval: Time interval (1min, 5min, 15min, 30min, 60min, daily, weekly, monthly)
            time_period: Number of data points used to calculate
            series_type: close, open, high, low
            
        Returns:
            Dictionary chứa dữ liệu indicator
        """
        logger.info(f"Fetching {function} for {symbol}")
        
        params = {
            'function': function,
            'symbol': symbol,
            'interval': interval,
            'time_period': time_period,
            'series_type': series_type
        }
        
        data = self._make_request(params)
        
        # Get the technical analysis key (it varies by indicator)
        tech_key = None
        for key in data.keys():
            if 'Technical Analysis' in key:
                tech_key = key
                break
        
        return {
            'symbol': symbol,
            'function': function,
            'metadata': data.get('Meta Data', {}),
            'technical_analysis': data.get(tech_key, {}),
            'extracted_at': datetime.utcnow().isoformat()
        }
    
    def get_earnings(self, symbol: str) -> Dict:
        """
        Lấy thông tin earnings
        
        Args:
            symbol: Mã cổ phiếu
            
        Returns:
            Dictionary chứa dữ liệu earnings
        """
        logger.info(f"Fetching earnings for {symbol}")
        
        params = {
            'function': 'EARNINGS',
            'symbol': symbol
        }
        
        data = self._make_request(params)
        
        return {
            'symbol': symbol,
            'annual_earnings': data.get('annualEarnings', []),
            'quarterly_earnings': data.get('quarterlyEarnings', []),
            'extracted_at': datetime.utcnow().isoformat()
        }
    
    def extract_comprehensive_data(
        self,
        symbol: str,
        include_indicators: bool = True,
        output_dir: str = '/tmp/alpha_vantage'
    ) -> str:
        """
        Trích xuất dữ liệu toàn diện cho một cổ phiếu
        
        Args:
            symbol: Mã cổ phiếu
            include_indicators: Có lấy technical indicators không
            output_dir: Thư mục lưu output
            
        Returns:
            File path của dữ liệu đã lưu
        """
        os.makedirs(output_dir, exist_ok=True)
        
        comprehensive_data = {
            'symbol': symbol,
            'extracted_at': datetime.utcnow().isoformat()
        }
        
        try:
            # Time series data
            comprehensive_data['time_series'] = self.get_time_series_daily(symbol)
            
            # Company overview
            comprehensive_data['company_overview'] = self.get_company_overview(symbol)
            
            # Earnings
            comprehensive_data['earnings'] = self.get_earnings(symbol)
            
            # News sentiment
            comprehensive_data['news_sentiment'] = self.get_news_sentiment(tickers=symbol, limit=50)
            
            # Technical indicators
            if include_indicators:
                comprehensive_data['indicators'] = {
                    'rsi': self.get_technical_indicator(symbol, 'RSI'),
                    'macd': self.get_technical_indicator(symbol, 'MACD'),
                    'sma_20': self.get_technical_indicator(symbol, 'SMA', time_period=20),
                    'ema_12': self.get_technical_indicator(symbol, 'EMA', time_period=12),
                }
            
            # Lưu vào file
            date_str = datetime.now().strftime('%Y-%m-%d')
            filename = f"{output_dir}/alpha_vantage_{symbol}_{date_str}.json"
            
            with open(filename, 'w') as f:
                json.dump(comprehensive_data, f, indent=2)
            
            logger.info(f"Saved comprehensive data for {symbol} to {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Failed to extract comprehensive data for {symbol}: {str(e)}")
            raise
    
    def extract_multiple_tickers(
        self,
        tickers: List[str],
        include_indicators: bool = False,
        output_dir: str = '/tmp/alpha_vantage'
    ) -> List[str]:
        """
        Trích xuất dữ liệu cho nhiều cổ phiếu
        
        Args:
            tickers: Danh sách mã cổ phiếu
            include_indicators: Có lấy technical indicators không
            output_dir: Thư mục lưu output
            
        Returns:
            List các file paths đã được tạo
        """
        created_files = []
        
        for ticker in tickers:
            try:
                filename = self.extract_comprehensive_data(
                    symbol=ticker,
                    include_indicators=include_indicators,
                    output_dir=output_dir
                )
                created_files.append(filename)
                
            except Exception as e:
                logger.error(f"Failed to extract data for {ticker}: {str(e)}")
                continue
        
        return created_files


def main():
    """
    Function chính để test extractor
    """
    # Test với một số ticker phổ biến
    tickers = ['AAPL', 'GOOGL']
    
    extractor = AlphaVantageExtractor()
    
    files = extractor.extract_multiple_tickers(
        tickers=tickers,
        include_indicators=False  # Set False để tránh rate limit khi test
    )
    
    print(f"\nExtracted {len(files)} files:")
    for file in files:
        print(f"  - {file}")


if __name__ == '__main__':
    main()
