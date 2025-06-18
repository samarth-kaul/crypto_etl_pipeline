<!-- Crypto Market Data Pipeline
A modular, configurable data engineering pipeline that extracts cryptocurrency market data from the CoinGecko API, transforms it (cleaning, filtering, aggregation), and stores it as Parquet files in AWS S3.
Features

Extract: Fetches structured data from CoinGecko API.
Transform: Cleans data, filters by market cap, and aggregates (e.g., average price by symbol).
Load: Stores processed data in AWS S3 as Parquet files.
Configurable: Uses YAML for pipeline configuration.
Production-Ready: Includes logging, error handling, and modular design.

Prerequisites

Python 3.9+
AWS account with S3 access and credentials configured
Install dependencies: pip install -r requirements.txt

Project Structure
crypto_pipeline/
├── config/
│   └── config.yaml           # Pipeline configuration
├── src/
│   ├── extract.py            # Data extraction logic
│   ├── transform.py          # Data transformation logic
│   ├── load.py               # Data loading to S3
│   └── utils.py              # Utility functions (logging, config)
├── tests/
│   └── test_pipeline.py      # Unit tests
├── main.py                   # Pipeline orchestration
├── requirements.txt          # Dependencies
├── .env                      # Environment variables
└── README.md                 # Project documentation

Setup

Clone the repository:git clone <repository-url>
cd crypto_pipeline


Install dependencies:pip install -r requirements.txt


Configure AWS credentials in ~/.aws/credentials.
Update config/config.yaml with your S3 bucket name and desired parameters.
Run the pipeline:python main.py



Configuration
Edit config/config.yaml to customize:

API endpoint and parameters
Transformation rules (e.g., min market cap, columns, aggregations)
S3 bucket and prefix
Logging settings

Future Improvements

Add unit tests in tests/test_pipeline.py.
Implement incremental data extraction.
Add data validation before loading to S3.
Deploy pipeline using Airflow or AWS Lambda.

License
MIT License -->




Crypto Data Pipeline
A professional data engineering project demonstrating a configurable ETL pipeline that extracts cryptocurrency market data from the CoinGecko API, transforms it (cleansing, filtering, aggregation), and stores the processed data as Parquet files in AWS S3.

Features
Extract: Fetches structured data from the CoinGecko API.
Transform: Cleans nulls, filters by market cap, selects columns, and aggregates metrics (e.g., average price, total volume).
Load: Stores processed data as Parquet files in an S3 bucket.
Configurable: Uses a YAML file for pipeline settings.
Observability: Implements logging for monitoring.
Modular: Follows clean architecture and SOLID principles.

Tech Stack
Python: Core programming language.
Libraries: requests, pandas, pyarrow, boto3, pyyaml, python-dotenv.
AWS S3: Storage for processed data.
CoinGecko API: Public API for cryptocurrency data.
YAML: Configuration management.
Project Structure
text



Project Structure
crypto-data-pipeline/
├── src/                    # Source code
│   ├── config.py           # Configuration loading
│   ├── extract.py          # API data extraction
│   ├── transform.py        # Data transformation
│   ├── load.py             # S3 storage
│   ├── pipeline.py         # ETL orchestration
├── config/                 # Configuration files
│   └── config.yaml
├── logs/                   # Log files
│   └── pipeline.log
├── .env                    # Environment variables
├── main.py                 # Entry point
├── requirements.txt        # Dependencies
├── README.md               # Documentation
└── .gitignore              # Git ignore


Setup Instructions

Clone the Repository:
git clone https://github.com/your-username/crypto-data-pipeline.git
cd crypto-data-pipeline

Set Up Virtual Environment:
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

Install Dependencies:
pip install -r requirements.txt

Configure AWS Credentials:
Create a .env file in the root directory.
Add your AWS credentials:
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1

Configure the Pipeline:
Update config/config.yaml with your S3 bucket name and desired parameters.

Run the Pipeline:
python main.py

Run Tests: 
pyest tests/ -v

Configuration
The pipeline is configured via config/config.yaml. Key sections:
api: CoinGecko API settings (endpoint, parameters).
transform: Transformation rules (min market cap, columns, aggregation).
s3: S3 bucket and prefix for storage.

Example Output
Parquet files are stored in S3 at: s3://your-bucket-name/crypto-data/processed/crypto_data_YYYYMMDD_HHMMSS.parquet.
Logs are saved in logs/pipeline.log.

Future Improvements
Add scheduling with Apache Airflow or AWS Lambda.
Implement data validation with Great Expectations.
Add unit tests with pytest.
Containerize with Docker for deployment.

License
MIT License
