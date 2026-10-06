import os
import time
import pandas as pd
import streamlit as st
import plotly.express as px


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Consumer Complaint Big Data Analytics",
    page_icon="",
    layout="wide"
)


# ============================================================
# PATHS
# ============================================================

BASE_PATH = r"C:\Users\Atharva\Desktop\Consumer Complaint Analytics"

PARQUET_PATH = os.path.join(
    BASE_PATH,
    "data",
    "processed",
    "complaints_parquet_1million"
)

RESULTS_PATH = os.path.join(
    BASE_PATH,
    "results"
)


# ============================================================
# HELPER FUNCTION
# ============================================================

@st.cache_data
def load_parquet(path):
    return pd.read_parquet(path)


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.title("BDA Analytics Platform")

st.sidebar.markdown(
    """
    ### Consumer Complaint Analytics

    **Big Data Processing**
    
    - PySpark
    - Spark SQL
    - Window Functions
    - Parquet
    - Partitioning
    - Statistical Analytics
    """
)

page = st.sidebar.radio(
    "Navigation",
    [
        "Dashboard",
        "Data Pipeline",
        "Data Quality",
        "Complaint Analytics",
        "Time-Series Analytics",
        "Geographic Analytics",
        "Multidimensional Analysis",
        "Pareto Analysis",
        "Anomaly Detection",
        "Performance",
        "Insights"
    ]
)


# ============================================================
# DASHBOARD
# ============================================================

if page == "Dashboard":

    st.title("Consumer Complaint Big Data Analytics Platform")

    st.markdown(
        "Large-scale consumer complaint analysis using Apache Spark"
    )

    st.divider()

    # --------------------------------------------------------
    # Load basic data
    # --------------------------------------------------------

    df = load_parquet(PARQUET_PATH)

    total_records = len(df)

    total_products = df["Product"].nunique()

    total_states = df["State"].nunique()

    total_companies = df["Company"].nunique()

    # --------------------------------------------------------
    # KPI CARDS
    # --------------------------------------------------------

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Total Complaints",
        f"{total_records:,}"
    )

    col2.metric(
        "Products",
        total_products
    )

    col3.metric(
        "States",
        total_states
    )

    col4.metric(
        "Companies",
        total_companies
    )

    st.divider()

    # --------------------------------------------------------
    # TOP PRODUCTS
    # --------------------------------------------------------

    st.subheader("Top Complaint Categories")

    product_counts = (
        df["Product"]
        .value_counts()
        .reset_index()
    )

    product_counts.columns = [
        "Product",
        "Complaint_Count"
    ]

    product_counts = product_counts.head(10)

    fig = px.bar(
        product_counts,
        x="Complaint_Count",
        y="Product",
        orientation="h",
        title="Top 10 Products by Complaint Volume"
    )

    fig.update_layout(
        yaxis=dict(categoryorder="total ascending"),
        height=500
    )

    st.plotly_chart(
        fig,
        use_container_width=True
    )

    # --------------------------------------------------------
    # YEARLY TREND
    # --------------------------------------------------------

    st.subheader("Complaint Trend by Year")

    yearly = (
        df.groupby("Year")
        .size()
        .reset_index(name="Complaint_Count")
        .sort_values("Year")
    )

    fig2 = px.line(
        yearly,
        x="Year",
        y="Complaint_Count",
        markers=True,
        title="Yearly Complaint Volume"
    )

    st.plotly_chart(
        fig2,
        use_container_width=True
    )

    # --------------------------------------------------------
    # SUBMISSION METHOD
    # --------------------------------------------------------

    col1, col2 = st.columns(2)

    with col1:

        st.subheader("Submission Methods")

        submission = (
            df["Submitted via"]
            .value_counts()
            .reset_index()
        )

        submission.columns = [
            "Submitted via",
            "Complaint_Count"
        ]

        fig3 = px.pie(
            submission,
            names="Submitted via",
            values="Complaint_Count",
            title="Complaint Submission Methods"
        )

        st.plotly_chart(
            fig3,
            use_container_width=True
        )

    # --------------------------------------------------------
    # COMPANY RESPONSE
    # --------------------------------------------------------

    with col2:

        st.subheader("Company Response")

        response = (
            df["Company response to consumer"]
            .value_counts()
            .reset_index()
        )

        response.columns = [
            "Company response to consumer",
            "Complaint_Count"
        ]

        fig4 = px.bar(
            response,
            x="Complaint_Count",
            y="Company response to consumer",
            orientation="h",
            title="Company Response Distribution"
        )

        st.plotly_chart(
            fig4,
            use_container_width=True
        )

    # --------------------------------------------------------
    # PROJECT INFORMATION
    # --------------------------------------------------------

    st.divider()

    st.subheader("Big Data Processing Architecture")

    st.code(
        """
Raw CFPB Complaint Data
        ↓
PySpark Ingestion
        ↓
Data Cleaning & ETL
        ↓
Transformation
        ↓
Year Partitioning
        ↓
Parquet Storage
        ↓
Spark SQL Analytics
        ↓
Window Functions
        ↓
Time-Series / Geographic /
Multidimensional Analytics
        ↓
Pareto & Statistical Anomaly Detection
        ↓
Streamlit Analytics Platform
        """,
        language="text"
    )


# ============================================================
# DATA PIPELINE
# ============================================================

elif page == "Data Pipeline":

    st.title("Data Pipeline")

    st.markdown(
        "End-to-end PySpark data processing pipeline"
    )

    st.divider()

    # --------------------------------------------------------
    # Dataset information
    # --------------------------------------------------------

    st.subheader("Dataset Overview")

    df = load_parquet(PARQUET_PATH)

    total_records = len(df)
    total_columns = len(df.columns)

    # Count Parquet files
    parquet_files = []

    for root, dirs, files in os.walk(PARQUET_PATH):
        for file in files:
            if file.endswith(".parquet"):
                parquet_files.append(
                    os.path.join(root, file)
                )

    total_parquet_files = len(parquet_files)

    # Dataset size
    total_size = 0

    for file_path in parquet_files:
        total_size += os.path.getsize(file_path)

    size_mb = total_size / (1024 * 1024)

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Records",
        f"{total_records:,}"
    )

    col2.metric(
        "Columns",
        total_columns
    )

    col3.metric(
        "Parquet Files",
        total_parquet_files
    )

    col4.metric(
        "Parquet Size",
        f"{size_mb:.1f} MB"
    )

    st.divider()

    # --------------------------------------------------------
    # Pipeline architecture
    # --------------------------------------------------------

    st.subheader("Processing Architecture")

    st.code(
        """
        CFPB Consumer Complaint Dataset
                    ↓
             Raw CSV Ingestion
                    ↓
              PySpark ETL
                    ↓
        ┌─────────────────────────┐
        │ Data Cleaning           │
        │ Missing Values          │
        │ Duplicate Detection     │
        │ Date Transformation     │
        │ Column Transformation   │
        └─────────────────────────┘
                    ↓
             Data Partitioning
                    ↓
             Parquet Storage
                    ↓
          Spark SQL Analytics
                    ↓
        Window / Time-Series /
        Geographic / Multidimensional
        Analytics
                    ↓
          Statistical Analytics
                    ↓
            Streamlit Dashboard
        """,
        language="text"
    )

    st.divider()

    # --------------------------------------------------------
    # ETL stages
    # --------------------------------------------------------

    st.subheader("ETL Processing Stages")

    etl_data = pd.DataFrame({
        "Stage": [
            "Data Ingestion",
            "Data Cleaning",
            "Duplicate Detection",
            "Date Transformation",
            "Feature Creation",
            "Partitioning",
            "Parquet Storage",
            "Spark Analytics"
        ],
        "Technology": [
            "PySpark",
            "PySpark",
            "PySpark",
            "PySpark",
            "PySpark",
            "Spark",
            "Parquet",
            "Spark SQL"
        ],
        "Status": [
            "Completed",
            "Completed",
            "Completed",
            "Completed",
            "Completed",
            "Completed",
            "Completed",
            "Completed"
        ]
    })

    st.dataframe(
        etl_data,
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # Partition information
    # --------------------------------------------------------

    st.subheader("Parquet Partition Structure")

    partition_years = []

    for item in os.listdir(PARQUET_PATH):

        if item.startswith("Year="):
            partition_years.append(
                item.replace("Year=", "")
            )

    partition_years = sorted(
        partition_years,
        key=lambda x: int(x)
    )

    if partition_years:

        partition_df = pd.DataFrame({
            "Partition Column": ["Year"] * len(partition_years),
            "Partition Value": partition_years
        })

        st.dataframe(
            partition_df,
            use_container_width=True,
            hide_index=True
        )

    else:

        st.info(
            "Partition directories could not be detected."
        )

    st.divider()

    # --------------------------------------------------------
    # Schema
    # --------------------------------------------------------

    st.subheader("Processed Dataset Schema")

    schema_df = pd.DataFrame({
        "Column": df.columns,
        "Data Type": [
            str(dtype)
            for dtype in df.dtypes
        ]
    })

    st.dataframe(
        schema_df,
        use_container_width=True,
        hide_index=True
    )
# ============================================================
# DATA QUALITY
# ============================================================

elif page == "Data Quality":

    st.title("Data Quality")

    st.markdown(
        "Data quality assessment of the processed consumer complaint dataset"
    )

    st.divider()

    # --------------------------------------------------------
    # Load processed data
    # --------------------------------------------------------

    df = load_parquet(PARQUET_PATH)

    # --------------------------------------------------------
    # Basic quality metrics
    # --------------------------------------------------------

    total_records = len(df)
    total_columns = len(df.columns)

    missing_values = int(df.isnull().sum().sum())
    duplicate_rows = int(df.duplicated().sum())

    total_cells = total_records * total_columns

    completeness = (
        ((total_cells - missing_values) / total_cells) * 100
        if total_cells > 0 else 0
    )

    # --------------------------------------------------------
    # KPI CARDS
    # --------------------------------------------------------

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Total Records",
        f"{total_records:,}"
    )

    col2.metric(
        "Total Columns",
        total_columns
    )

    col3.metric(
        "Missing Values",
        f"{missing_values:,}"
    )

    col4.metric(
        "Completeness",
        f"{completeness:.2f}%"
    )

    st.divider()

    # --------------------------------------------------------
    # Duplicate analysis
    # --------------------------------------------------------

    st.subheader("Duplicate Analysis")

    duplicate_percentage = (
        (duplicate_rows / total_records) * 100
        if total_records > 0 else 0
    )

    d1, d2, d3 = st.columns(3)

    d1.metric(
        "Duplicate Records",
        f"{duplicate_rows:,}"
    )

    d2.metric(
        "Duplicate Percentage",
        f"{duplicate_percentage:.2f}%"
    )

    if duplicate_rows == 0:
        d3.metric(
            "Duplicate Status",
            "Clean"
        )
    else:
        d3.metric(
            "Duplicate Status",
            "Duplicates Found"
        )

    st.divider()

    # --------------------------------------------------------
    # Missing values by column
    # --------------------------------------------------------

    st.subheader("Missing Values by Column")

    missing_df = pd.DataFrame({
        "Column": df.columns,
        "Missing Values": [
            int(df[column].isnull().sum())
            for column in df.columns
        ]
    })

    missing_df["Missing Percentage"] = (
        missing_df["Missing Values"] / total_records * 100
    ).round(2)

    missing_df = missing_df.sort_values(
        "Missing Values",
        ascending=False
    )

    st.dataframe(
        missing_df,
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # Column completeness
    # --------------------------------------------------------

    st.subheader("Column Completeness")

    completeness_df = missing_df.copy()

    completeness_df["Completeness Percentage"] = (
        100 - completeness_df["Missing Percentage"]
    ).round(2)

    completeness_df = completeness_df[
        [
            "Column",
            "Completeness Percentage"
        ]
    ]

    st.dataframe(
        completeness_df,
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # Unique values
    # --------------------------------------------------------

    st.subheader("Unique Values by Column")

    unique_df = pd.DataFrame({
        "Column": df.columns,
        "Unique Values": [
            int(df[column].nunique(dropna=True))
            for column in df.columns
        ]
    })

    unique_df = unique_df.sort_values(
        "Unique Values",
        ascending=False
    )

    st.dataframe(
        unique_df,
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # Data quality status
    # --------------------------------------------------------

    st.subheader("Data Quality Status")

    quality_checks = pd.DataFrame({
        "Quality Check": [
            "Dataset available",
            "Records available",
            "Duplicate records",
            "Missing values",
            "Date column available",
            "Year column available",
            "Month column available"
        ],
       "Status": [
    "PASS" if total_records > 0 else "FAIL",
    "PASS" if total_records > 0 else "FAIL",
    "PASS" if duplicate_rows == 0 else "CHECK",
    "PASS" if completeness >= 95 else "CHECK" if completeness >= 80 else "FAIL",
    "PASS" if "Date received" in df.columns else "FAIL",
    "PASS" if "Year" in df.columns else "FAIL",
    "PASS" if "Month" in df.columns else "FAIL"
]
    })

    st.dataframe(
        quality_checks,
        use_container_width=True,
        hide_index=True
    )
# ============================================================
# COMPLAINT ANALYTICS
# ============================================================

elif page == "Complaint Analytics":

    st.title("Complaint Analytics")

    st.markdown(
        "Large-scale complaint analysis using Spark-generated data"
    )

    st.divider()

    # --------------------------------------------------------
    # Load processed data
    # --------------------------------------------------------

    df = load_parquet(PARQUET_PATH)

    # --------------------------------------------------------
    # KPI METRICS
    # --------------------------------------------------------

    total_complaints = len(df)

    total_products = df["Product"].nunique()

    total_companies = df["Company"].nunique()

    total_states = df["State"].nunique()

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Total Complaints",
        f"{total_complaints:,}"
    )

    c2.metric(
        "Products",
        total_products
    )

    c3.metric(
        "Companies",
        total_companies
    )

    c4.metric(
        "States",
        total_states
    )

    st.divider()

    # --------------------------------------------------------
    # PRODUCT ANALYSIS
    # --------------------------------------------------------

    st.subheader("Complaints by Product")

    product_counts = (
        df["Product"]
        .value_counts()
        .reset_index()
    )

    product_counts.columns = [
        "Product",
        "Complaint Count"
    ]

    st.bar_chart(
        product_counts.set_index("Product")
    )

    st.divider()

    # --------------------------------------------------------
    # TOP COMPANIES
    # --------------------------------------------------------

    st.subheader("Top 15 Companies by Complaint Volume")

    company_counts = (
        df["Company"]
        .value_counts()
        .head(15)
        .reset_index()
    )

    company_counts.columns = [
        "Company",
        "Complaint Count"
    ]

    st.dataframe(
        company_counts,
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # SUBMISSION METHOD
    # --------------------------------------------------------

    st.subheader("Complaint Submission Methods")

    submission_counts = (
        df["Submitted via"]
        .value_counts()
        .reset_index()
    )

    submission_counts.columns = [
        "Submission Method",
        "Complaint Count"
    ]

    st.bar_chart(
        submission_counts.set_index("Submission Method")
    )

    st.divider()

    # --------------------------------------------------------
    # COMPANY RESPONSE
    # --------------------------------------------------------

    st.subheader("Company Response to Consumer")

    response_counts = (
        df["Company response to consumer"]
        .value_counts()
        .reset_index()
    )

    response_counts.columns = [
        "Company Response",
        "Complaint Count"
    ]

    st.dataframe(
        response_counts,
        use_container_width=True,
        hide_index=True
    )
# ============================================================
# TIME-SERIES ANALYTICS
# ============================================================

elif page == "Time-Series Analytics":

    st.title("Time-Series Analytics")

    st.markdown(
        "Temporal analysis of consumer complaint volumes"
    )

    st.divider()

    # --------------------------------------------------------
    # Load processed data
    # --------------------------------------------------------

    df = load_parquet(PARQUET_PATH)

    # --------------------------------------------------------
    # MONTHLY TREND
    # --------------------------------------------------------

    st.subheader("Monthly Complaint Trend")

    monthly = (
        df.groupby(["Year", "Month"])
        .size()
        .reset_index(name="Complaint Count")
        .sort_values(["Year", "Month"])
    )

    monthly["Period"] = (
        monthly["Year"].astype(str)
        + "-"
        + monthly["Month"].astype(str).str.zfill(2)
    )

    st.line_chart(
        monthly.set_index("Period")["Complaint Count"]
    )

    st.divider()

    # --------------------------------------------------------
    # YEARLY TREND
    # --------------------------------------------------------

    st.subheader("Yearly Complaint Trend")

    yearly = (
        df.groupby("Year")
        .size()
        .reset_index(name="Complaint Count")
        .sort_values("Year")
    )

    st.bar_chart(
        yearly.set_index("Year")["Complaint Count"]
    )

    st.divider()

    # --------------------------------------------------------
    # MONTH-OVER-MONTH GROWTH
    # --------------------------------------------------------

    st.subheader("Month-over-Month Growth")

    monthly["Previous Month"] = (
        monthly["Complaint Count"].shift(1)
    )

    monthly["MoM Growth %"] = (
        (
            (monthly["Complaint Count"] - monthly["Previous Month"])
            / monthly["Previous Month"]
        ) * 100
    ).round(2)

    st.dataframe(
        monthly[
            [
                "Period",
                "Complaint Count",
                "Previous Month",
                "MoM Growth %"
            ]
        ],
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # 3-MONTH ROLLING AVERAGE
    # --------------------------------------------------------

    st.subheader("3-Month Rolling Average")

    monthly["3-Month Rolling Average"] = (
        monthly["Complaint Count"]
        .rolling(3)
        .mean()
        .round(2)
    )

    rolling_chart = monthly[
        [
            "Period",
            "Complaint Count",
            "3-Month Rolling Average"
        ]
    ].set_index("Period")

    st.line_chart(rolling_chart)

    st.divider()

    # --------------------------------------------------------
    # PEAK COMPLAINT MONTHS
    # --------------------------------------------------------

    st.subheader("Peak Complaint Months")

    peak_months = (
        monthly
        .sort_values("Complaint Count", ascending=False)
        .head(10)
    )

    st.dataframe(
        peak_months[
            [
                "Period",
                "Complaint Count"
            ]
        ],
        use_container_width=True,
        hide_index=True
    )
# ============================================================
# GEOGRAPHIC ANALYTICS
# ============================================================

elif page == "Geographic Analytics":

    st.title("Geographic Analytics")

    st.markdown(
        "Geographic distribution of consumer complaints across U.S. states"
    )

    st.divider()

    # --------------------------------------------------------
    # Load processed data
    # --------------------------------------------------------

    df = load_parquet(PARQUET_PATH)

    # --------------------------------------------------------
    # KPI METRICS
    # --------------------------------------------------------

    total_states = df["State"].nunique()

    top_state = (
        df["State"]
        .value_counts()
        .idxmax()
    )

    top_state_count = (
        df["State"]
        .value_counts()
        .max()
    )

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "States Represented",
        total_states
    )

    c2.metric(
        "Highest Complaint State",
        top_state
    )

    c3.metric(
        "Complaints in Top State",
        f"{top_state_count:,}"
    )

    st.divider()

    # --------------------------------------------------------
    # COMPLAINTS BY STATE
    # --------------------------------------------------------

    st.subheader("Complaints by State")

    state_counts = (
        df["State"]
        .value_counts()
        .reset_index()
    )

    state_counts.columns = [
        "State",
        "Complaint Count"
    ]

    st.bar_chart(
        state_counts.set_index("State")["Complaint Count"]
    )

    st.divider()

    # --------------------------------------------------------
    # TOP 15 STATES
    # --------------------------------------------------------

    st.subheader("Top 15 States by Complaint Volume")

    top_states = (
        state_counts
        .head(15)
        .copy()
    )

    st.dataframe(
        top_states,
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # PRODUCT × STATE ANALYSIS
    # --------------------------------------------------------

    st.subheader("Product × State Analysis")

    selected_state = st.selectbox(
        "Select a State",
        sorted(df["State"].dropna().unique())
    )

    state_product = (
        df[df["State"] == selected_state]
        .groupby("Product")
        .size()
        .reset_index(name="Complaint Count")
        .sort_values("Complaint Count", ascending=False)
        .head(10)
    )

    st.dataframe(
        state_product,
        use_container_width=True,
        hide_index=True
    )

    st.bar_chart(
        state_product.set_index("Product")["Complaint Count"]
    )

    st.divider()

    # --------------------------------------------------------
    # STATE × SUBMISSION METHOD
    # --------------------------------------------------------

    st.subheader("State × Submission Method")

    selected_state_method = st.selectbox(
        "Select State for Submission Analysis",
        sorted(df["State"].dropna().unique()),
        key="submission_state"
    )

    state_submission = (
        df[df["State"] == selected_state_method]
        .groupby("Submitted via")
        .size()
        .reset_index(name="Complaint Count")
        .sort_values("Complaint Count", ascending=False)
    )

    st.dataframe(
        state_submission,
        use_container_width=True,
        hide_index=True
    )

    st.bar_chart(
        state_submission.set_index("Submitted via")["Complaint Count"]
    )
# ============================================================
# MULTIDIMENSIONAL ANALYSIS
# ============================================================

elif page == "Multidimensional Analysis":

    st.title("Multidimensional Analysis")

    st.markdown(
        "Multi-dimensional analysis of complaints across products, states, companies and submission methods"
    )

    st.divider()

    # --------------------------------------------------------
    # Load processed data
    # --------------------------------------------------------

    df = load_parquet(PARQUET_PATH)

    # --------------------------------------------------------
    # PRODUCT × SUBMISSION METHOD
    # --------------------------------------------------------

    st.subheader("Product × Submission Method")

    product_submission = (
        df.groupby(["Product", "Submitted via"])
        .size()
        .reset_index(name="Complaint Count")
        .sort_values("Complaint Count", ascending=False)
    )

    st.dataframe(
        product_submission.head(50),
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # PRODUCT × COMPANY RESPONSE
    # --------------------------------------------------------

    st.subheader("Product × Company Response")

    product_response = (
        df.groupby(["Product", "Company response to consumer"])
        .size()
        .reset_index(name="Complaint Count")
        .sort_values("Complaint Count", ascending=False)
    )

    st.dataframe(
        product_response.head(50),
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # STATE × PRODUCT
    # --------------------------------------------------------

    st.subheader("State × Product")

    state_product = (
        df.groupby(["State", "Product"])
        .size()
        .reset_index(name="Complaint Count")
        .sort_values("Complaint Count", ascending=False)
    )

    st.dataframe(
        state_product.head(50),
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # YEAR × PRODUCT
    # --------------------------------------------------------

    st.subheader("Year × Product")

    year_product = (
        df.groupby(["Year", "Product"])
        .size()
        .reset_index(name="Complaint Count")
        .sort_values(
            ["Year", "Complaint Count"],
            ascending=[True, False]
        )
    )

    st.dataframe(
        year_product.head(50),
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # INTERACTIVE PRODUCT ANALYSIS
    # --------------------------------------------------------

    st.subheader("Interactive Product Analysis")

    selected_product = st.selectbox(
        "Select Product",
        sorted(df["Product"].dropna().unique())
    )

    product_filtered = df[
        df["Product"] == selected_product
    ]

    m1, m2, m3 = st.columns(3)

    m1.metric(
        "Complaints",
        f"{len(product_filtered):,}"
    )

    m2.metric(
        "States",
        product_filtered["State"].nunique()
    )

    m3.metric(
        "Companies",
        product_filtered["Company"].nunique()
    )

    st.divider()

    # --------------------------------------------------------
    # SELECTED PRODUCT × STATE
    # --------------------------------------------------------

    selected_product_states = (
        product_filtered["State"]
        .value_counts()
        .head(15)
        .reset_index()
    )

    selected_product_states.columns = [
        "State",
        "Complaint Count"
    ]

    st.subheader(
        f"Top States for {selected_product}"
    )

    st.bar_chart(
        selected_product_states.set_index("State")
    )
# ============================================================
# PARETO ANALYSIS
# ============================================================

elif page == "Pareto Analysis":

    st.title("Pareto Analysis")

    st.markdown(
        "Identifying the complaint categories contributing most to the total volume"
    )

    st.divider()

    # --------------------------------------------------------
    # Load processed data
    # --------------------------------------------------------

    df = load_parquet(PARQUET_PATH)

    # --------------------------------------------------------
    # PRODUCT PARETO
    # --------------------------------------------------------

    st.subheader("Product Pareto Analysis")

    pareto = (
        df.groupby("Product")
        .size()
        .reset_index(name="Complaint Count")
        .sort_values("Complaint Count", ascending=False)
    )

    total = pareto["Complaint Count"].sum()

    pareto["Percentage of Total"] = (
        pareto["Complaint Count"] / total * 100
    ).round(2)

    pareto["Cumulative Percentage"] = (
        pareto["Percentage of Total"].cumsum()
    ).round(2)

    st.dataframe(
        pareto,
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # TOP PRODUCTS
    # --------------------------------------------------------

    st.subheader("Top Products by Complaint Volume")

    top_products = pareto.head(10)

    st.bar_chart(
        top_products.set_index("Product")["Complaint Count"]
    )

    st.divider()

    # --------------------------------------------------------
    # 80% CONTRIBUTION POINT
    # --------------------------------------------------------

    st.subheader("80% Contribution Analysis")

    products_for_80 = pareto[
        pareto["Cumulative Percentage"] <= 80
    ]

    # Include the first category that crosses 80%
    if len(products_for_80) < len(pareto):
        next_row = pareto.iloc[len(products_for_80):len(products_for_80)+1]
        products_for_80 = pd.concat(
            [products_for_80, next_row]
        )

    st.metric(
        "Products Responsible for Approximately 80% of Complaints",
        len(products_for_80)
    )

    st.dataframe(
        products_for_80[
            [
                "Product",
                "Complaint Count",
                "Percentage of Total",
                "Cumulative Percentage"
            ]
        ],
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # ISSUE-LEVEL PARETO
    # --------------------------------------------------------

    st.subheader("Issue-Level Pareto Analysis")

    issue_pareto = (
        df.groupby("Issue")
        .size()
        .reset_index(name="Complaint Count")
        .sort_values("Complaint Count", ascending=False)
    )

    issue_total = issue_pareto["Complaint Count"].sum()

    issue_pareto["Percentage of Total"] = (
        issue_pareto["Complaint Count"] / issue_total * 100
    ).round(2)

    issue_pareto["Cumulative Percentage"] = (
        issue_pareto["Percentage of Total"].cumsum()
    ).round(2)

    st.dataframe(
        issue_pareto.head(30),
        use_container_width=True,
        hide_index=True
    )
# ============================================================
# ANOMALY DETECTION
# ============================================================

elif page == "Anomaly Detection":

    st.title("Anomaly Detection")

    st.markdown(
        "Statistical detection of unusual complaint-volume periods using Z-score analysis"
    )

    st.divider()

    # --------------------------------------------------------
    # Load processed data
    # --------------------------------------------------------

    df = load_parquet(PARQUET_PATH)

    # --------------------------------------------------------
    # MONTHLY COMPLAINT COUNTS
    # --------------------------------------------------------

    monthly = (
        df.groupby(["Year", "Month"])
        .size()
        .reset_index(name="Complaint Count")
        .sort_values(["Year", "Month"])
    )

    # --------------------------------------------------------
    # STATISTICAL BASELINE
    # --------------------------------------------------------

    mean_complaints = monthly["Complaint Count"].mean()
    std_complaints = monthly["Complaint Count"].std()

    monthly["Z-Score"] = (
        (monthly["Complaint Count"] - mean_complaints)
        / std_complaints
    ).round(2)

    # --------------------------------------------------------
    # ANOMALY FLAG
    # --------------------------------------------------------

    monthly["Anomaly"] = monthly["Z-Score"].abs() >= 2

    # --------------------------------------------------------
    # PERIOD
    # --------------------------------------------------------

    monthly["Period"] = (
        monthly["Year"].astype(str)
        + "-"
        + monthly["Month"].astype(str).str.zfill(2)
    )

    # --------------------------------------------------------
    # KPI METRICS
    # --------------------------------------------------------

    anomaly_count = int(monthly["Anomaly"].sum())

    highest_zscore = monthly["Z-Score"].max()

    average_monthly = round(mean_complaints, 2)

    c1, c2, c3 = st.columns(3)

    c1.metric(
        "Average Monthly Complaints",
        f"{average_monthly:,.0f}"
    )

    c2.metric(
        "Detected Anomalies",
        anomaly_count
    )

    c3.metric(
        "Highest Z-Score",
        f"{highest_zscore:.2f}"
    )

    st.divider()

    # --------------------------------------------------------
    # ANOMALOUS PERIODS
    # --------------------------------------------------------

    st.subheader("Detected Anomalous Periods")

    anomalies = (
        monthly[
            monthly["Anomaly"] == True
        ]
        .sort_values("Z-Score", ascending=False)
    )

    st.dataframe(
        anomalies[
            [
                "Period",
                "Complaint Count",
                "Z-Score",
                "Anomaly"
            ]
        ],
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # COMPLAINT TREND
    # --------------------------------------------------------

    st.subheader("Complaint Volume with Statistical Baseline")

    trend = monthly[
        [
            "Period",
            "Complaint Count"
        ]
    ].set_index("Period")

    st.line_chart(trend)

    st.divider()

    # --------------------------------------------------------
    # Z-SCORE ANALYSIS
    # --------------------------------------------------------

    st.subheader("Z-Score Analysis")

    zscore_data = monthly[
        [
            "Period",
            "Z-Score"
        ]
    ].set_index("Period")

    st.bar_chart(zscore_data)

    st.caption(
        "A period is classified as anomalous when its absolute Z-score is greater than or equal to 2."
    )

# ============================================================
# PERFORMANCE
# ============================================================

elif page == "Performance":

    st.title("Performance Analysis")

    st.markdown(
        "Performance measurements for large-scale complaint data processing"
    )

    st.divider()

    # --------------------------------------------------------
    # LOAD PROCESSED PARQUET DATA + READ TIME
    # --------------------------------------------------------

    start_time = time.time()

    performance_df = pd.read_parquet(PARQUET_PATH)

    read_time = time.time() - start_time

    # --------------------------------------------------------
    # DATASET METRICS
    # --------------------------------------------------------

    total_records = len(performance_df)
    total_columns = len(performance_df.columns)

    # --------------------------------------------------------
    # COUNT PARQUET FILES
    # --------------------------------------------------------

    parquet_files = 0

    if os.path.exists(PARQUET_PATH):

        for root, dirs, files in os.walk(PARQUET_PATH):

            parquet_files += sum(
                1
                for file in files
                if file.endswith(".parquet")
            )

    # --------------------------------------------------------
    # PERFORMANCE TEST 1 - PRODUCT AGGREGATION
    # --------------------------------------------------------

    start_time = time.time()

    _ = (
        performance_df
        .groupby("Product")
        .size()
        .sort_values(ascending=False)
    )

    aggregation_time = time.time() - start_time

    # --------------------------------------------------------
    # PERFORMANCE TEST 2 - MULTIDIMENSIONAL QUERY
    # --------------------------------------------------------

    start_time = time.time()

    _ = (
        performance_df
        .groupby(["State", "Product"])
        .size()
        .sort_values(ascending=False)
    )

    multidimensional_time = time.time() - start_time

    # --------------------------------------------------------
    # KPI CARDS
    # --------------------------------------------------------

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Records Processed",
        f"{total_records:,}"
    )

    c2.metric(
        "Columns",
        total_columns
    )

    c3.metric(
        "Parquet Files",
        parquet_files
    )

    c4.metric(
        "Read Time",
        f"{read_time:.2f} sec"
    )

    st.divider()

    # --------------------------------------------------------
    # PROCESSING TIMES
    # --------------------------------------------------------

    st.subheader("Processing Performance")

    performance_times = pd.DataFrame({
        "Operation": [
            "Dataset Read",
            "Product Aggregation",
            "Multidimensional Query"
        ],
        "Execution Time (seconds)": [
            round(read_time, 3),
            round(aggregation_time, 3),
            round(multidimensional_time, 3)
        ]
    })

    st.dataframe(
        performance_times,
        use_container_width=True,
        hide_index=True
    )

    st.bar_chart(
        performance_times.set_index("Operation")
    )

    st.divider()

    # --------------------------------------------------------
    # DATASET SCALE
    # --------------------------------------------------------

    st.subheader("Dataset Scale")

    scale_df = pd.DataFrame({
        "Metric": [
            "Total Records",
            "Total Columns",
            "Parquet Files",
            "Number of Partitions"
        ],
        "Value": [
            f"{total_records:,}",
            total_columns,
            parquet_files,
            performance_df["Year"].nunique()
        ]
    })

    st.dataframe(
        scale_df,
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # SCALABILITY NOTE
    # --------------------------------------------------------

    st.subheader("Scalability")

    st.info(
        "The platform uses PySpark and Parquet to support scalable "
        "processing of large consumer complaint datasets. "
        "Local execution time may not always outperform Pandas "
        "for smaller datasets because Spark introduces processing overhead."
    )
# ============================================================
# INSIGHTS
# ============================================================

elif page == "Insights":

    st.title("Key Insights")

    st.markdown(
        "Summary of major findings from the consumer complaint dataset"
    )

    st.divider()

    # Load processed data
    insights_df = load_parquet(PARQUET_PATH)

    # --------------------------------------------------------
    # TOP PRODUCT
    # --------------------------------------------------------

    top_product = (
        insights_df
        .groupby("Product")
        .size()
        .sort_values(ascending=False)
        .reset_index(name="Complaint_Count")
    )

    top_product_name = top_product.iloc[0]["Product"]
    top_product_count = int(top_product.iloc[0]["Complaint_Count"])

    # --------------------------------------------------------
    # TOP STATE
    # --------------------------------------------------------

    top_state = (
        insights_df
        .groupby("State")
        .size()
        .sort_values(ascending=False)
        .reset_index(name="Complaint_Count")
    )

    top_state_name = top_state.iloc[0]["State"]
    top_state_count = int(top_state.iloc[0]["Complaint_Count"])

    # --------------------------------------------------------
    # TOP COMPANY
    # --------------------------------------------------------

    top_company = (
        insights_df
        .groupby("Company")
        .size()
        .sort_values(ascending=False)
        .reset_index(name="Complaint_Count")
    )

    top_company_name = top_company.iloc[0]["Company"]
    top_company_count = int(top_company.iloc[0]["Complaint_Count"])

    # --------------------------------------------------------
    # TOTAL COMPLAINTS
    # --------------------------------------------------------

    total_complaints = len(insights_df)

    # --------------------------------------------------------
    # KPI CARDS
    # --------------------------------------------------------

    c1, c2, c3, c4 = st.columns(4)

    c1.metric(
        "Total Complaints",
        f"{total_complaints:,}"
    )

    c2.metric(
        "Top Product",
        top_product_name
    )

    c3.metric(
        "Top State",
        top_state_name
    )

    c4.metric(
        "Top Company",
        top_company_name
    )

    st.divider()

    # --------------------------------------------------------
    # TOP PRODUCTS
    # --------------------------------------------------------

    st.subheader("Top Complaint Products")

    st.dataframe(
        top_product.head(10),
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # TOP STATES
    # --------------------------------------------------------

    st.subheader("Top Complaint States")

    st.dataframe(
        top_state.head(10),
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # TOP COMPANIES
    # --------------------------------------------------------

    st.subheader("Top Companies by Complaint Volume")

    st.dataframe(
        top_company.head(10),
        use_container_width=True,
        hide_index=True
    )

    st.divider()

    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------

    st.subheader("Summary")

    st.info(
        f"The dataset contains {total_complaints:,} consumer complaints. "
        f"The highest complaint volume is associated with "
        f"'{top_product_name}', while '{top_state_name}' "
        f"has the highest number of complaints by state. "
        f"'{top_company_name}' is the company with the highest "
        f"complaint volume."
    )
