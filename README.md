\# Consumer Complaint Big Data Analytics Platform



A Big Data Analytics platform for analyzing large-scale consumer complaint data using \*\*PySpark, Spark SQL, Parquet, and Streamlit\*\*.



\## Project Overview



This project processes and analyzes a large consumer complaint dataset and provides an interactive web dashboard for exploring complaint patterns and trends.



The project focuses on Big Data Analytics techniques such as:



\- PySpark-based data processing

\- ETL and data cleaning

\- Parquet columnar storage

\- Data partitioning

\- Spark SQL

\- Window functions

\- Time-series analytics

\- Geographic analytics

\- Multidimensional analysis

\- Pareto analysis

\- Statistical anomaly detection

\- Performance analysis

\- Interactive Streamlit dashboard



\*\*No Machine Learning or NLP techniques are used.\*\*



\---



\## Dataset



The project was tested using \*\*1,000,000 consumer complaint records\*\*.



The dataset contains information such as:



\- Complaint ID

\- Product

\- State

\- Company

\- Submission Method

\- Company Response

\- Date Received

\- Other complaint-related attributes



The large dataset and processed Parquet files are intentionally \*\*not included in this GitHub repository\*\* because of GitHub storage limitations.



\---



\## System Architecture



```text

Consumer Complaint Dataset

&#x20;       |

&#x20;       v

PySpark Ingestion

&#x20;       |

&#x20;       v

Data Cleaning

&#x20;       |

&#x20;       v

Transformation

&#x20;       |

&#x20;       v

Parquet Storage

&#x20;       |

&#x20;       v

Partitioning by Year

&#x20;       |

&#x20;       v

Spark SQL

&#x20;       |

&#x20;       v

Big Data Analytics

&#x20;       |

&#x20;       v

Streamlit Dashboard

```



\---



\## Analytics Modules



\### 1. ETL Pipeline



\- Data ingestion

\- Data cleaning

\- Missing value handling

\- Duplicate detection

\- Data type conversion

\- Date transformation

\- Feature and column creation

\- Parquet generation



\### 2. Data Quality



\- Missing value analysis

\- Completeness analysis

\- Duplicate analysis

\- Column-level quality checks

\- Unique value analysis



\### 3. Spark SQL



\- Grouping

\- Aggregations

\- Sorting

\- Multidimensional queries



\### 4. Window Analytics



\- Product rankings

\- Running totals

\- Previous-period comparison

\- Month-over-month growth

\- Percentage contribution

\- Cumulative contribution



\### 5. Time-Series Analytics



\- Monthly trends

\- Yearly trends

\- Month-over-month growth

\- Year-over-year growth

\- Rolling averages

\- Peak periods

\- Seasonal patterns



\### 6. Geographic Analytics



\- State-wise complaint analysis

\- State contribution

\- Product × State

\- State × Submission Method

\- State × Company Response

\- Top product by state



\### 7. Multidimensional Analysis



\- Product × State

\- Product × Submission Method

\- Product × Company Response

\- Year × Product

\- Year × State

\- Product × State × Submission Method



\### 8. Pareto Analysis



Pareto analysis identifies the products responsible for the largest proportion of total complaints using cumulative percentage analysis.



\### 9. Statistical Anomaly Detection



Statistical Z-score analysis is used to identify unusually high or low complaint volumes.



The anomaly detection module does \*\*not use Machine Learning\*\*.



\### 10. Performance Analysis



The performance module measures:



\- Dataset size

\- Number of records

\- Number of columns

\- Number of Parquet files

\- Parquet read performance

\- Aggregation performance

\- Multidimensional query performance



\---



\## Project Structure



```text

Consumer-Complaint-Big-Data-Analytics/

│

├── app.py

├── main.py

├── requirements.txt

├── README.md

├── .gitignore

│

├── pipeline/

│   ├── etl.py

│   ├── data\_quality.py

│   ├── spark\_sql\_analytics.py

│   ├── window\_analytics.py

│   ├── time\_series.py

│   ├── geographic\_analytics.py

│   ├── multidimensional.py

│   ├── pareto\_analysis.py

│   ├── anomaly\_detection.py

│   └── performance\_analysis.py

│

├── data/

│   └── processed/

│

└── results/

```



Large datasets, processed Parquet files, Spark temporary files, and generated result files are excluded from the GitHub repository.



\---



\## Technologies Used



\- \*\*Python\*\*

\- \*\*PySpark\*\*

\- \*\*Apache Spark\*\*

\- \*\*Spark SQL\*\*

\- \*\*Pandas\*\*

\- \*\*PyArrow\*\*

\- \*\*Plotly\*\*

\- \*\*Streamlit\*\*

\- \*\*Parquet\*\*

\- \*\*Git \& GitHub\*\*



\---



\## Requirements



\- Python 3.x

\- Java 17

\- Apache Spark / PySpark 4.2.0

\- Streamlit

\- Pandas

\- PyArrow

\- Plotly



\---



\## Installation



\### 1. Clone the Repository



```bash

git clone https://github.com/atharvaaa05/Consumer-Complaint-Big-Data-Analytics.git

```



\### 2. Navigate to the Project Directory



```bash

cd Consumer-Complaint-Big-Data-Analytics

```



\### 3. Install Python Dependencies



```bash

pip install -r requirements.txt

```



\### 4. Configure Java and PySpark



Make sure Java 17 and PySpark are installed and configured correctly before running the Big Data processing pipeline.



\---



\## Running the Dashboard



After placing the processed Parquet dataset in the required project directory, run:



```bash

python -m streamlit run app.py

```



The Streamlit dashboard will open in the browser.



\---



\## Running the Analytics Pipeline



The individual Big Data Analytics modules are available inside the `pipeline` directory.



The pipeline can be executed locally using PySpark after configuring the required dataset and Java/Spark environment.



The main analytics modules include:



```text

pipeline/

├── etl.py

├── data\_quality.py

├── spark\_sql\_analytics.py

├── window\_analytics.py

├── time\_series.py

├── geographic\_analytics.py

├── multidimensional.py

├── pareto\_analysis.py

├── anomaly\_detection.py

└── performance\_analysis.py

```



\---



\## Big Data Concepts Demonstrated



This project demonstrates the following Big Data concepts:



\- Distributed data processing

\- ETL

\- Data cleaning

\- Columnar storage

\- Parquet

\- Data partitioning

\- Spark SQL

\- Aggregations

\- Window functions

\- Multidimensional analysis

\- Time-series analysis

\- Geographic analysis

\- Pareto analysis

\- Statistical anomaly detection

\- Performance measurement

\- Interactive data visualization



\---



\## Dashboard Features



The Streamlit dashboard provides the following sections:



1\. Dashboard

2\. Data Pipeline

3\. Data Quality

4\. Complaint Analytics

5\. Time-Series Analytics

6\. Geographic Analytics

7\. Multidimensional Analysis

8\. Pareto Analysis

9\. Anomaly Detection

10\. Performance

11\. Insights



\---



\## Key Project Highlights



\- Processes and analyzes \*\*1 million consumer complaint records\*\*

\- Uses \*\*PySpark for Big Data processing\*\*

\- Uses \*\*Parquet for columnar storage\*\*

\- Uses \*\*Year-based partitioning\*\*

\- Uses \*\*Spark SQL and Window Functions\*\*

\- Performs multidimensional and time-series analytics

\- Performs statistical anomaly detection without Machine Learning

\- Provides an interactive Streamlit dashboard

\- Includes performance analysis of Big Data operations



\---



\## Project Objective



The objective of this project is to demonstrate how large-scale consumer complaint data can be processed, stored, queried, and analyzed using Big Data technologies to identify meaningful patterns, trends, distributions, anomalies, and relationships across multiple dimensions.



