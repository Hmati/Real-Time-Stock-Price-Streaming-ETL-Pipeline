# Real-Time-Stock--Streaming-ETL-PipelinePrice

## Overview
This project demonstrates an end-to-end ETL pipeline that streams real-time stock price data, processes it for analytics, and stores it in cloud-based storage and a data warehouse. The pipeline is scalable, leveraging tools like Apache Kafka, Apache Spark, and cloud platforms.

---

## Objective
To ingest, process, and store streaming stock price data in real time using modern ETL practices. The target storage includes cloud-based storage (Amazon S3/Google Cloud Storage) and a cloud data warehouse (Amazon Redshift/Google BigQuery) for analytics.

---

## Architecture
1. **Data Source**: Stock price data is fetched in real-time from the Alpha Vantage API.
2. **Data Ingestion**: Apache Kafka is used to ingest streaming data.
3. **ETL Processing**: Apache Spark Structured Streaming processes and transforms the data.
4. **Cloud Storage**: Processed data is stored in Amazon S3 or Google Cloud Storage.
5. **Data Warehouse**: Data is loaded into Amazon Redshift or Google BigQuery for analytics.
6. **Visualization**: Tableau or Google Data Studio dashboards for insights.

---

## Tools and Technologies
- **Programming Language**: Python (for Kafka Producer and Spark ETL).
- **Streaming Framework**: Apache Kafka.
- **ETL Processing**: Apache Spark Structured Streaming.
- **Cloud Storage**: Amazon S3 or Google Cloud Storage.
- **Data Warehouse**: Amazon Redshift or Google BigQuery.
- **Orchestration**: Apache Airflow.
- **Visualization**: Tableau or Google Data Studio.

---

## Prerequisites
1. **Alpha Vantage API Key**: Register at [Alpha Vantage](https://www.alphavantage.co/).
2. **Kafka**: Installed and configured.
3. **Spark**: Installed with Structured Streaming support.
4. **AWS/GCP Account**: For cloud storage and data warehouse.
5. **Airflow**: Installed for orchestration.

---

## Setup and Execution

### Step 1: Fetch Real-Time Data
- Register for an Alpha Vantage API key.
- Use the provided Python script (`kafka_producer.py`) to fetch stock data and send it to a Kafka topic.

```bash
python kafka_producer.py
```

### Step 2: Configure Kafka
- Set up a Kafka topic named `stock_prices`.
- Ensure Kafka is running locally or on a configured cluster.

### Step 3: Stream Processing with Spark
- Use the provided PySpark script (`spark_etl.py`) to process streaming data from Kafka and write it to cloud storage.

```bash
spark-submit spark_etl.py
```

### Step 4: Load Data into Cloud Data Warehouse
- Use AWS Glue or a provided Airflow DAG to load data from cloud storage (S3/GCS) to Redshift or BigQuery.

```bash
python airflow_dag.py
```

### Step 5: Visualization
- Connect your data warehouse to Tableau or Google Data Studio.
- Build real-time dashboards for stock price trends and insights.

---

## File Structure
```
├── kafka_producer.py       # Script to fetch and stream data to Kafka
├── spark_etl.py            # Spark Structured Streaming ETL job
├── airflow_dag.py          # Airflow DAG for data pipeline orchestration
├── requirements.txt        # Python dependencies
├── README.md               # Project documentation
```

---

## Sample Data Schema
| Column     | Type      | Description           |
|------------|-----------|-----------------------|
| symbol     | String    | Stock ticker symbol  |
| price      | Float     | Stock price          |
| timestamp  | Timestamp | Data timestamp       |

---

## Future Enhancements
1. Add monitoring with Prometheus or CloudWatch.
2. Implement alert systems for significant stock price movements.
3. Integrate advanced analytics and machine learning for stock predictions.

---

## Author
Hellen Mati

---

## License
This project is licensed under the MIT License. See the LICENSE file for details.
