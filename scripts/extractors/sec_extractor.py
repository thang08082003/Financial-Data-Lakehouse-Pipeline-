"""
SEC API Extractor
Thu thập dữ liệu hồ sơ pháp lý từ SEC EDGAR API
"""

import os
import json
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SECExtractor:
    """
    Extractor để lấy dữ liệu từ SEC EDGAR API
    """
    
    BASE_URL = "https://data.sec.gov"
    
    def __init__(self, user_agent: Optional[str] = None):
        """
        Khởi tạo SEC extractor
        
        Args:
            user_agent: User agent cho requests (SEC yêu cầu user agent)
        """
        # SEC requires a user agent
        if not user_agent:
            user_agent = os.getenv('SEC_USER_AGENT', 'Financial Lakehouse Pipeline contact@example.com')
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'data.sec.gov'
        })
        
        # SEC rate limit: 10 requests per second
        self.rate_limit_delay = 0.1
    
    def _make_request(self, url: str) -> Dict:
        """
        Thực hiện API request với rate limiting
        
        Args:
            url: URL để request
            
        Returns:
            API response as dictionary
        """
        try:
            response = self.session.get(url)
            response.raise_for_status()
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
            
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Request error: {str(e)}")
            raise
    
    def get_company_tickers(self) -> Dict:
        """
        Lấy danh sách tất cả company tickers
        
        Returns:
            Dictionary mapping CIK to ticker information
        """
        logger.info("Fetching company tickers")
        
        url = f"{self.BASE_URL}/files/company_tickers.json"
        data = self._make_request(url)
        
        return {
            'tickers': data,
            'extracted_at': datetime.utcnow().isoformat()
        }
    
    def get_company_cik(self, ticker: str) -> Optional[str]:
        """
        Lấy CIK number từ ticker symbol
        
        Args:
            ticker: Mã cổ phiếu
            
        Returns:
            CIK number (padded to 10 digits) hoặc None
        """
        tickers_data = self.get_company_tickers()
        
        for key, company in tickers_data['tickers'].items():
            if company.get('ticker', '').upper() == ticker.upper():
                cik = str(company.get('cik_str', '')).zfill(10)
                logger.info(f"Found CIK {cik} for ticker {ticker}")
                return cik
        
        logger.warning(f"CIK not found for ticker {ticker}")
        return None
    
    def get_company_facts(self, cik: str) -> Dict:
        """
        Lấy company facts (dữ liệu tài chính chuẩn hóa)
        
        Args:
            cik: CIK number (10 digits)
            
        Returns:
            Dictionary chứa company facts
        """
        logger.info(f"Fetching company facts for CIK {cik}")
        
        url = f"{self.BASE_URL}/api/xbrl/companyfacts/CIK{cik}.json"
        
        try:
            data = self._make_request(url)
            return {
                'cik': cik,
                'facts': data,
                'extracted_at': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Error fetching company facts for CIK {cik}: {str(e)}")
            return {
                'cik': cik,
                'facts': {},
                'error': str(e),
                'extracted_at': datetime.utcnow().isoformat()
            }
    
    def get_company_concept(
        self,
        cik: str,
        taxonomy: str = 'us-gaap',
        concept: str = 'Revenue'
    ) -> Dict:
        """
        Lấy dữ liệu cho một financial concept cụ thể
        
        Args:
            cik: CIK number
            taxonomy: XBRL taxonomy (us-gaap, ifrs-full, dei, srt)
            concept: Financial concept (Revenue, Assets, NetIncomeLoss, etc.)
            
        Returns:
            Dictionary chứa concept data
        """
        logger.info(f"Fetching {taxonomy}:{concept} for CIK {cik}")
        
        url = f"{self.BASE_URL}/api/xbrl/companyconcept/CIK{cik}/{taxonomy}/{concept}.json"
        
        try:
            data = self._make_request(url)
            return {
                'cik': cik,
                'taxonomy': taxonomy,
                'concept': concept,
                'data': data,
                'extracted_at': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Error fetching concept for CIK {cik}: {str(e)}")
            return {
                'cik': cik,
                'taxonomy': taxonomy,
                'concept': concept,
                'data': {},
                'error': str(e),
                'extracted_at': datetime.utcnow().isoformat()
            }
    
    def get_submissions(self, cik: str) -> Dict:
        """
        Lấy tất cả submissions (filings) của một công ty
        
        Args:
            cik: CIK number
            
        Returns:
            Dictionary chứa submissions data
        """
        logger.info(f"Fetching submissions for CIK {cik}")
        
        url = f"{self.BASE_URL}/submissions/CIK{cik}.json"
        
        try:
            data = self._make_request(url)
            return {
                'cik': cik,
                'submissions': data,
                'extracted_at': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Error fetching submissions for CIK {cik}: {str(e)}")
            return {
                'cik': cik,
                'submissions': {},
                'error': str(e),
                'extracted_at': datetime.utcnow().isoformat()
            }
    
    def get_recent_filings(
        self,
        cik: str,
        form_types: List[str] = None,
        limit: int = 10
    ) -> List[Dict]:
        """
        Lấy các filings gần nhất của một công ty
        
        Args:
            cik: CIK number
            form_types: List các loại form (10-K, 10-Q, 8-K, etc.)
            limit: Số lượng filings tối đa
            
        Returns:
            List các filings
        """
        if form_types is None:
            form_types = ['10-K', '10-Q', '8-K']
        
        submissions = self.get_submissions(cik)
        
        if 'error' in submissions or not submissions.get('submissions'):
            return []
        
        recent_filings = submissions['submissions'].get('filings', {}).get('recent', {})
        
        filings = []
        for i in range(len(recent_filings.get('form', []))):
            form_type = recent_filings['form'][i]
            
            if form_type in form_types:
                filing = {
                    'accessionNumber': recent_filings.get('accessionNumber', [])[i],
                    'filingDate': recent_filings.get('filingDate', [])[i],
                    'reportDate': recent_filings.get('reportDate', [])[i],
                    'form': form_type,
                    'fileNumber': recent_filings.get('fileNumber', [])[i],
                    'filmNumber': recent_filings.get('filmNumber', [])[i],
                    'primaryDocument': recent_filings.get('primaryDocument', [])[i],
                    'primaryDocDescription': recent_filings.get('primaryDocDescription', [])[i],
                }
                filings.append(filing)
                
                if len(filings) >= limit:
                    break
        
        return filings
    
    def extract_comprehensive_data(
        self,
        ticker: str,
        output_dir: str = '/tmp/sec'
    ) -> Optional[str]:
        """
        Trích xuất dữ liệu toàn diện cho một công ty
        
        Args:
            ticker: Mã cổ phiếu
            output_dir: Thư mục lưu output
            
        Returns:
            File path của dữ liệu đã lưu hoặc None nếu failed
        """
        os.makedirs(output_dir, exist_ok=True)
        
        # Lấy CIK từ ticker
        cik = self.get_company_cik(ticker)
        if not cik:
            logger.error(f"Cannot find CIK for ticker {ticker}")
            return None
        
        comprehensive_data = {
            'ticker': ticker,
            'cik': cik,
            'extracted_at': datetime.utcnow().isoformat()
        }
        
        try:
            # Company facts
            comprehensive_data['facts'] = self.get_company_facts(cik)
            
            # Submissions
            comprehensive_data['submissions'] = self.get_submissions(cik)
            
            # Recent filings
            comprehensive_data['recent_filings'] = self.get_recent_filings(cik, limit=20)
            
            # Key financial concepts
            comprehensive_data['concepts'] = {
                'revenue': self.get_company_concept(cik, 'us-gaap', 'Revenues'),
                'net_income': self.get_company_concept(cik, 'us-gaap', 'NetIncomeLoss'),
                'assets': self.get_company_concept(cik, 'us-gaap', 'Assets'),
                'liabilities': self.get_company_concept(cik, 'us-gaap', 'Liabilities'),
            }
            
            # Lưu vào file
            date_str = datetime.now().strftime('%Y-%m-%d')
            filename = f"{output_dir}/sec_{ticker}_{date_str}.json"
            
            with open(filename, 'w') as f:
                json.dump(comprehensive_data, f, indent=2)
            
            logger.info(f"Saved comprehensive SEC data for {ticker} to {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Failed to extract comprehensive data for {ticker}: {str(e)}")
            return None
    
    def extract_multiple_tickers(
        self,
        tickers: List[str],
        output_dir: str = '/tmp/sec'
    ) -> List[str]:
        """
        Trích xuất dữ liệu cho nhiều công ty
        
        Args:
            tickers: Danh sách mã cổ phiếu
            output_dir: Thư mục lưu output
            
        Returns:
            List các file paths đã được tạo
        """
        created_files = []
        
        for ticker in tickers:
            try:
                filename = self.extract_comprehensive_data(
                    ticker=ticker,
                    output_dir=output_dir
                )
                if filename:
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
    tickers = ['AAPL', 'MSFT']
    
    extractor = SECExtractor()
    
    files = extractor.extract_multiple_tickers(tickers=tickers)
    
    print(f"\nExtracted {len(files)} files:")
    for file in files:
        print(f"  - {file}")


if __name__ == '__main__':
    main()
