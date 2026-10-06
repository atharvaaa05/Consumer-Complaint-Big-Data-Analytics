\# Consumer Complaint Big Data Analytics Platform



A Big Data Analytics platform for analyzing large-scale consumer complaint data using PySpark, Spark SQL, Parquet and Streamlit.



\## Project Overview



This project processes and analyzes a large consumer complaint dataset and provides an interactive web dashboard for exploring complaint patterns.



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



No Machine Learning or NLP techniques are used.



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



The large dataset is intentionally not included in this GitHub repository because of GitHub storage limitations.



\## System Architecture



```text

Consumer Complaint Dataset

&#x20;         |

&#x20;         v

&#x20;   PySpark Ingestion

&#x20;         |

&#x20;         v

&#x20;    Data Cleaning

&#x20;         |

&#x20;         v

&#x20;     Transformation

&#x20;         |

&#x20;         v

&#x20;  Parquet Storage

&#x20;         |

&#x20;         v

&#x20;    Partitioning

&#x20;      by Year

&#x20;         |

&#x20;         v

&#x20;     Spark SQL

&#x20;         |

&#x20;         v

&#x20;  Big Data Analytics

&#x20;         |

&#x20;         v

&#x20;  Streamlit Dashboard

**Analytics Modules**


1\. ETL Pipeline

\- Data ingestion

\- Data cleaning

\- Missing value handling

\- Duplicate detection

\- Data type conversion

\- Date transformation

\- Feature/column creation

\- Parquet generation

2\. Data Quality

\- Missing value analysis

\- Completeness analysis

\- Duplicate analysis

\- Column-level quality checks

\- Unique value analysis

3\. Spark SQL

\- Grouping

\- Aggregations

\- Sorting

\- Multidimensional queries

4\. Window Analytics

\- Rankings

\- Running totals

\- Previous-period comparison

\- MoM growth

\- Percentage contribution

\- Cumulative contribution

5\. Time-Series Analytics

\- Monthly trends

\- Yearly trends

\- Month-over-month growth

\- Year-over-year growth

\- Rolling averages

\- Peak periods

\- Seasonal patterns

6\. Geographic Analytics

\- State-wise complaint analysis

\- State contribution

\- Product × State

\- State × Submission Method

\- State × Company Response

7\. Multidimensional Analysis

\- Product × State

\- Product × Submission Method

\- Product × Company Response

\- Year × Product

\- Year × State

\- Product × State × Submission Method

8\. Pareto Analysis

Identifies the products responsible for the largest proportion of total complaints using cumulative percentage analysis.

9\. Statistical Anomaly Detection

Uses statistical Z-score analysis to identify unusually high or low complaint volumes.

No machine learning is used.

10\. Performance Analysis

Measures:

\- Dataset size

\- Number of records

\- Number of columns

\- Parquet files

\- Read performance

\- Aggregation performance

\- Multidimensional query performance

**Project Structure:**

Consumer-Complaint-Big-Data-Analytics/

│

├── app.py

├── main.py

├── requirements.txt

├── README.md

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

Large datasets, processed Parquet files and generated results are excluded from GitHub.

**Requirements:**


\- Python 3.x

\- Java 17

\- Apache Spark / PySpark 4.2.0

\- Streamlit

\- Pandas

\- PyArrow

\- Plotly

**Installation:**


Clone the repository:
git clone https://github.com/atharvaaa05/Consumer-Complaint-Big-Data-Analytics.git

Navigate into the project:
cd Consumer-Complaint-Big-Data-Analytics

Install dependencies:
pip install -r requirements.txt

Running the Dashboard

After placing the processed Parquet dataset in the required project directory, run:
python -m streamlit run app.py

The dashboard will open in the browser.


Running the Analytics Pipeline

The individual analytics modules are available inside the pipeline directory.

The pipeline can be executed locally using PySpark after configuring the required dataset and Spark/Java environment.

**Technologies Used:**


\- Python

\- PySpark

\- Apache Spark

\- Spark SQL

\- Pandas

\- PyArrow

\- Plotly

\- Streamlit

\- Parquet

\- Git/GitHub


**Big Data Concepts Demonstrated:**


\- Distributed data processing

\- ETL

\- Columnar storage

\- Data partitioning

\- SQL-based analytics

\- Window functions

\- Aggregations

\- Multidimensional analysis

\- Time-series analysis

\- Statistical analysis

\- Performance measurement





