"""
Polygon.io API Extractor
Thu thập dữ liệu giá cổ phiếu từ Polygon API
"""

import os
import json
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PolygonExtractor:
    """
    Extractor để lấy dữ liệu từ Polygon.io API
    """
    
    BASE_URL = "https://api.polygon.io"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Khởi tạo Polygon extractor
        
        Args:
            api_key: API key của Polygon.io
        """
        self.api_key = api_key or os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("POLYGON_API_KEY không được cung cấp")
        
        self.session = requests.Session()
        self.session.params = {'apiKey': self.api_key}
    
    def get_aggregates(
        self,
        ticker: str,
        multiplier: int = 1,
        timespan: str = 'day',
        from_date: str = None,
        to_date: str = None
    ) -> Dict:
        """
        Lấy dữ liệu aggregate bars (OHLCV)
        
        Args:
            ticker: Mã cổ phiếu (vd: AAPL)
            multiplier: Kích thước của timespan (vd: 1)
            timespan: Timespan (minute, hour, day, week, month, quarter, year)
            from_date: Ngày bắt đầu (YYYY-MM-DD)
            to_date: Ngày kết thúc (YYYY-MM-DD)
            
        Returns:
            Dictionary chứa dữ liệu aggregate
        """
        if not from_date:
            from_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        if not to_date:
            to_date = datetime.now().strftime('%Y-%m-%d')
        
        url = f"{self.BASE_URL}/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
        
        try:
            logger.info(f"Fetching aggregates for {ticker} from {from_date} to {to_date}")
            response = self.session.get(url, params={'adjusted': 'true', 'sort': 'asc'})
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') == 'OK' and data.get('results'):
                logger.info(f"Successfully fetched {len(data['results'])} records for {ticker}")
                return {
                    'ticker': ticker,
                    'results': data['results'],
                    'resultsCount': data.get('resultsCount', 0),
                    'queryCount': data.get('queryCount', 0),
                    'extracted_at': datetime.utcnow().isoformat()
                }
            else:
                logger.warning(f"No data found for {ticker}")
                return {'ticker': ticker, 'results': [], 'extracted_at': datetime.utcnow().isoformat()}
                
        except requests.RequestException as e:
            logger.error(f"Error fetching data for {ticker}: {str(e)}")
            raise
    
    def get_ticker_details(self, ticker: str) -> Dict:
        """
        Lấy thông tin chi tiết về cổ phiếu
        
        Args:
            ticker: Mã cổ phiếu
            
        Returns:
            Dictionary chứa thông tin chi tiết
        """
        url = f"{self.BASE_URL}/v3/reference/tickers/{ticker}"
        
        try:
            logger.info(f"Fetching ticker details for {ticker}")
            response = self.session.get(url)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') == 'OK':
                return {
                    'ticker': ticker,
                    'details': data.get('results', {}),
                    'extracted_at': datetime.utcnow().isoformat()
                }
            else:
                logger.warning(f"No details found for {ticker}")
                return {'ticker': ticker, 'details': {}, 'extracted_at': datetime.utcnow().isoformat()}
                
        except requests.RequestException as e:
            logger.error(f"Error fetching details for {ticker}: {str(e)}")
            raise
    
    def get_trades(
        self,
        ticker: str,
        timestamp: Optional[str] = None,
        limit: int = 1000
    ) -> Dict:
        """
        Lấy dữ liệu trades cho một ngày cụ thể
        
        Args:
            ticker: Mã cổ phiếu
            timestamp: Timestamp (YYYY-MM-DD)
            limit: Số lượng trades tối đa
            
        Returns:
            Dictionary chứa dữ liệu trades
        """
        if not timestamp:
            timestamp = datetime.now().strftime('%Y-%m-%d')
        
        url = f"{self.BASE_URL}/v3/trades/{ticker}"
        
        try:
            logger.info(f"Fetching trades for {ticker} on {timestamp}")
            response = self.session.get(
                url,
                params={
                    'timestamp': timestamp,
                    'limit': limit,
                    'sort': 'timestamp'
                }
            )
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') == 'OK':
                return {
                    'ticker': ticker,
                    'timestamp': timestamp,
                    'results': data.get('results', []),
                    'extracted_at': datetime.utcnow().isoformat()
                }
            else:
                return {
                    'ticker': ticker,
                    'timestamp': timestamp,
                    'results': [],
                    'extracted_at': datetime.utcnow().isoformat()
                }
                
        except requests.RequestException as e:
            logger.error(f"Error fetching trades for {ticker}: {str(e)}")
            raise
    
    def extract_multiple_tickers(
        self,
        tickers: List[str],
        from_date: str = None,
        to_date: str = None,
        output_dir: str = '/tmp/polygon'
    ) -> List[str]:
        """
        Trích xuất dữ liệu cho nhiều cổ phiếu và lưu vào files
        
        Args:
            tickers: Danh sách các mã cổ phiếu
            from_date: Ngày bắt đầu
            to_date: Ngày kết thúc
            output_dir: Thư mục lưu output
            
        Returns:
            List các file paths đã được tạo
        """
        os.makedirs(output_dir, exist_ok=True)
        created_files = []
        
        for ticker in tickers:
            try:
                # Lấy aggregates data
                aggregates = self.get_aggregates(ticker, from_date=from_date, to_date=to_date)
                
                # Lấy ticker details
                details = self.get_ticker_details(ticker)
                
                # Combine data
                combined_data = {
                    'ticker': ticker,
                    'aggregates': aggregates,
                    'details': details,
                    'extracted_at': datetime.utcnow().isoformat()
                }
                
                # Lưu vào file JSON
                date_str = datetime.now().strftime('%Y-%m-%d')
                filename = f"{output_dir}/polygon_{ticker}_{date_str}.json"
                
                with open(filename, 'w') as f:
                    json.dump(combined_data, f, indent=2)
                
                created_files.append(filename)
                logger.info(f"Saved data for {ticker} to {filename}")
                
            except Exception as e:
                logger.error(f"Failed to extract data for {ticker}: {str(e)}")
                continue
        
        return created_files


def main():
    """
    Function chính để test extractor
    """
    # Test với một số ticker phổ biến
    tickers = ['AAPL', 'GOOGL', 'MSFT']
    
    extractor = PolygonExtractor()
    
    # Extract dữ liệu 30 ngày gần nhất
    from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    to_date = datetime.now().strftime('%Y-%m-%d')
    
    files = extractor.extract_multiple_tickers(
        tickers=tickers,
        from_date=from_date,
        to_date=to_date
    )
    
    print(f"\nExtracted {len(files)} files:")
    for file in files:
        print(f"  - {file}")


if __name__ == '__main__':
    main()
