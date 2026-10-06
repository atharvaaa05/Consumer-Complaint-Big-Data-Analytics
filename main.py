import pandas as pd
import matplotlib.pyplot as plt
import os

DATA_URL = "https://raw.githubusercontent.com/maha-prathamesh/Consumer-Complaints_Classification/main/Consumer_complaints.csv"

DATA_FOLDER = "data"
RESULTS_FOLDER = "results"
DATA_FILE = os.path.join(DATA_FOLDER, "complaints.csv")


def load_data():
    os.makedirs(DATA_FOLDER, exist_ok=True)
    os.makedirs(RESULTS_FOLDER, exist_ok=True)

    if not os.path.exists(DATA_FILE):
        print("\nDownloading complaint dataset...")
        df = pd.read_csv(DATA_URL)
        df.to_csv(DATA_FILE, index=False)
        print("Dataset downloaded successfully!")
    else:
        df = pd.read_csv(DATA_FILE)
        print("Dataset loaded from local storage.")

    return df


def dataset_overview(df):
    print("\n========== DATASET OVERVIEW ==========")
    print("Total complaints:", len(df))
    print("Total columns:", len(df.columns))

    print("\nColumns:")
    for column in df.columns:
        print("-", column)


def category_analysis(df):
    print("\n========== COMPLAINT CATEGORY ANALYSIS ==========")

    result = df["Product"].value_counts().head(10)

    print(result)

    result.plot(kind="bar", figsize=(10, 5))
    plt.title("Top 10 Complaint Categories")
    plt.xlabel("Product")
    plt.ylabel("Number of Complaints")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    file_path = os.path.join(
        RESULTS_FOLDER,
        "top_complaint_categories.png"
    )

    plt.savefig(file_path)
    plt.close()

    print("\nChart saved:", file_path)


def submission_analysis(df):
    print("\n========== SUBMISSION METHOD ANALYSIS ==========")

    result = df["Submitted via"].value_counts()

    print(result)

    result.plot(kind="bar", figsize=(8, 5))
    plt.title("Complaints by Submission Method")
    plt.xlabel("Submission Method")
    plt.ylabel("Number of Complaints")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    file_path = os.path.join(
        RESULTS_FOLDER,
        "submission_methods.png"
    )

    plt.savefig(file_path)
    plt.close()

    print("\nChart saved:", file_path)


def response_analysis(df):
    print("\n========== COMPANY RESPONSE ANALYSIS ==========")

    result = df["Company response to consumer"].value_counts()

    print(result)

    result.plot(kind="bar", figsize=(9, 5))
    plt.title("Company Response to Consumer")
    plt.xlabel("Company Response")
    plt.ylabel("Number of Complaints")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    file_path = os.path.join(
        RESULTS_FOLDER,
        "company_response.png"
    )

    plt.savefig(file_path)
    plt.close()

    print("\nChart saved:", file_path)


def main():

    print("========================================")
    print("   CONSUMER COMPLAINT ANALYTICS SYSTEM")
    print("========================================")

    df = load_data()

    while True:

        print("\n========================================")
        print("              MAIN MENU")
        print("========================================")

        print("1. Dataset Overview")
        print("2. Complaint Category Analysis")
        print("3. Submission Method Analysis")
        print("4. Company Response Analysis")
        print("5. Exit")

        choice = input("\nEnter your choice: ")

        if choice == "1":
            dataset_overview(df)

        elif choice == "2":
            category_analysis(df)

        elif choice == "3":
            submission_analysis(df)

        elif choice == "4":
            response_analysis(df)

        elif choice == "5":
            print("\nExiting application...")
            break

        else:
            print("\nInvalid choice. Please enter a number from 1 to 5.")


if __name__ == "__main__":
    main()