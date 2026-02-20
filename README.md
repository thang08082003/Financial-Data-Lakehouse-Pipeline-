# Financial Data Lakehouse Pipeline

## 📋 Mô tả dự án

Hệ thống tự động thu thập, lưu trữ và phân tích dữ liệu từ các nguồn API tài chính để phân tích mối tương quan giữa tin tức (Sentiment) và biến động giá cổ phiếu.

## 🏗️ Kiến trúc hệ thống

```
┌─────────────────┐
│   Data Sources  │
│  - Polygon API  │
│  - Alpha Vantage│
│  - SEC API      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Python ETL     │
│  (Extraction)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Hadoop HDFS    │
│  (Data Lake)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Apache Hive    │
│  (Structure)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Apache Spark   │
│  (Processing)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PostgreSQL     │
│  (Analytics DB) │
└─────────────────┘

Apache Airflow điều phối toàn bộ pipeline
```

## 🚀 Công nghệ sử dụng

- **Python 3.9+**: Ngôn ngữ lập trình chính
- **Apache Airflow 2.7+**: Orchestration và scheduling
- **Hadoop HDFS 3.3+**: Distributed storage
- **Apache Hive 3.1+**: Data warehousing
- **Apache Spark 3.4+**: Distributed processing
- **PostgreSQL 15+**: Analytical database
- **Docker & Docker Compose**: Containerization

## 📁 Cấu trúc dự án

```
Financial-Data-Lakehouse-Pipeline/
├── docker/                     # Docker configurations
│   ├── airflow/
│   ├── hadoop/
│   ├── hive/
│   └── spark/
├── dags/                       # Airflow DAGs
│   ├── financial_data_pipeline.py
│   └── data_quality_check.py
├── scripts/                    # Python scripts
│   ├── extractors/            # API data extractors
│   │   ├── polygon_extractor.py
│   │   ├── alpha_vantage_extractor.py
│   │   └── sec_extractor.py
│   ├── spark_jobs/            # Spark processing jobs
│   │   ├── data_cleaning.py
│   │   ├── data_transformation.py
│   │   └── sentiment_analysis.py
│   └── utils/                 # Utility functions
│       ├── hdfs_utils.py
│       ├── hive_utils.py
│       └── config.py
├── sql/                        # SQL scripts
│   ├── hive_schemas.sql
│   └── postgresql_schemas.sql
├── config/                     # Configuration files
│   ├── airflow.cfg
│   ├── hive-site.xml
│   └── spark-defaults.conf
├── tests/                      # Unit tests
├── docs/                       # Documentation
├── requirements.txt
├── docker-compose.yml
└── README.md
```

## 🔧 Cài đặt và Chạy

### Yêu cầu hệ thống

- Docker Desktop 4.0+
- RAM: Tối thiểu 16GB (khuyến nghị 32GB)
- Disk: Tối thiểu 50GB trống
- CPU: 4 cores+

### Bước 1: Clone và cấu hình

```bash
cd "d:\Financial Data Lakehouse Pipeline"

# Tạo file .env cho API keys
cp .env.example .env
# Chỉnh sửa .env với API keys của bạn
```

### Bước 2: Khởi động hệ thống

```bash
# Build và start tất cả services
docker-compose up -d

# Kiểm tra trạng thái
docker-compose ps
```

### Bước 3: Truy cập các services

- **Airflow UI**: http://localhost:8080 (user: admin, password: admin)
- **Spark Master UI**: http://localhost:8081
- **Hadoop NameNode UI**: http://localhost:9870
- **Hive Server**: localhost:10000

## 📊 Data Flow

### 1. Extraction (Python)
```python
# Thu thập dữ liệu từ APIs
- Polygon API: Stock prices, trades, quotes
- Alpha Vantage: Technical indicators, fundamental data
- SEC API: Company filings, news
```

### 2. Load to HDFS
```bash
# Dữ liệu JSON được lưu vào HDFS
/data/raw/
  ├── polygon/YYYY-MM-DD/
  ├── alpha_vantage/YYYY-MM-DD/
  └── sec/YYYY-MM-DD/
```

### 3. Structure with Hive
```sql
-- Tạo external tables trên HDFS
-- Partition theo ngày/tháng
-- Optimize với ORC format
```

### 4. Transform with Spark
```python
# Data cleaning, deduplication
# Feature engineering
# Sentiment scoring
# Correlation analysis
```

### 5. Load to PostgreSQL
```sql
-- Lưu kết quả phân tích
-- Tạo aggregated tables
-- Create views cho reporting
```

## 📚 API Data Sources

### Polygon.io
- **Endpoint**: Stock aggregates, trades, quotes
- **Frequency**: Real-time to daily
- **Data**: OHLCV, volume, trades

### Alpha Vantage
- **Endpoint**: TIME_SERIES_DAILY, NEWS_SENTIMENT
- **Frequency**: Daily
- **Data**: Technical indicators, news sentiment

### SEC EDGAR
- **Endpoint**: Company filings
- **Frequency**: As filed
- **Data**: 10-K, 10-Q, 8-K filings

## 🔄 Airflow Pipeline Schedule

```python
# Main pipeline: Chạy hàng ngày lúc 6:00 AM
DAG: financial_data_pipeline
Schedule: 0 6 * * *

Tasks:
1. extract_polygon_data
2. extract_alpha_vantage_data
3. extract_sec_data
4. load_to_hdfs
5. create_hive_tables
6. spark_data_cleaning
7. spark_transformation
8. spark_sentiment_analysis
9. load_to_postgresql
10. data_quality_check
```

## 🧪 Testing

```bash
# Chạy unit tests
pytest tests/

# Test Airflow DAG
python dags/financial_data_pipeline.py

# Test Spark job locally
spark-submit scripts/spark_jobs/data_cleaning.py
```

## 📈 Monitoring và Logging

- **Airflow Logs**: Xem trong UI hoặc `/logs` directory
- **Spark Logs**: Xem trong Spark UI
- **HDFS Health**: Hadoop NameNode UI
- **Database Metrics**: PostgreSQL queries

## 🎓 Kiến thức học được

### 1. Big Data Architecture
- Thiết kế Data Lake với HDFS
- ELT vs ETL patterns
- Data partitioning strategies

### 2. Distributed Computing
- Spark RDD, DataFrame operations
- Parallel processing optimization
- Resource management

### 3. Data Warehousing
- Hive table design
- Partitioning và bucketing
- Query optimization

### 4. Workflow Orchestration
- Airflow DAG design
- Task dependencies
- Error handling và retry logic

### 5. Containerization
- Multi-container applications
- Service networking
- Volume management

## 🐛 Troubleshooting

### HDFS không start được
```bash
# Format namenode (CHỈ LẦN ĐẦU)
docker-compose exec namenode hdfs namenode -format
```

### Airflow scheduler không chạy
```bash
# Restart scheduler
docker-compose restart airflow-scheduler
```

### Spark job bị lỗi memory
```bash
# Tăng memory trong spark-defaults.conf
spark.executor.memory=4g
spark.driver.memory=2g
```

## 📖 Tài liệu tham khảo

- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Apache Spark Docs](https://spark.apache.org/docs/latest/)
- [Hadoop HDFS Docs](https://hadoop.apache.org/docs/stable/)
- [Apache Hive Docs](https://hive.apache.org/)

## 🤝 Contributing

Dự án này là để học tập. Bạn có thể:
1. Fork project
2. Thêm features mới
3. Improve performance
4. Fix bugs

## 📝 License

MIT License - Free to use for learning purposes

## 👤 Author

Dự án thực hành Big Data & Data Engineering

---

**Happy Learning! 🚀**
