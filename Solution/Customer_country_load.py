from pyspark.sql import SparkSession
from pyspark.sql.functions import col, datediff, current_date, to_date, floor, when

# Read the customer consultation data from a CSV file
def read_data(file_path: str):
    return spark.read.option("delimiter", "|").csv(file_path, header=True, inferSchema=True)

# Preprocess the DataFrame by converting date formats and calculating new columns.
def preprocess_data(df):
    df = df.withColumn("Open_Date", to_date(col("Open_Date"), "yyyyMMdd")) \
           .withColumn("Last_Consulted_Date", to_date(col("Last_Consulted_Date"), "yyyyMMdd")) \
           .withColumn("DOB", to_date(col("DOB"), "ddMMyyyy")) \
           .withColumn("Age", floor(datediff(current_date(), col("DOB")) / 365)) \
           .withColumn("Days_Since_Last_Consulted", datediff(current_date(), col("Last_Consulted_Date"))) \
           .withColumn("Flag_Consulted_Over_30_Days", (col("Days_Since_Last_Consulted") > 30).cast("int"))
    return df

# Data Validation
def validate_data(df):
    """Validate the DataFrame for null values and future consultation dates."""
    valid_df = df.filter(col("Customer_Name").isNotNull() &
                         col("Customer_Id").isNotNull() &
                         col("Open_Date").isNotNull())
    valid_df = valid_df.withColumn("Is_Valid_Consultation", 
                                    when(col("Last_Consulted_Date") <= current_date(), True).otherwise(False))
    return valid_df.filter(col("Is_Valid_Consultation") == True)

# Loading data country wise
def save_country_data(df):
    """Save DataFrame to country-specific tables."""
    countries = df.select("Country").distinct().collect()

    for country_row in countries:
        country = country_row["Country"]
        country_df = df.filter(col("Country") == country)
        # Create or update the corresponding country-specific table
        country_df.write.mode("append").saveAsTable(f"Table_{country}")

if __name__ == "__main__":

    FILE_PATH = "../data/Customer_consultation_date.csv"
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Hospital Customer Data Processing") \
        .getOrCreate()

    df = read_data(FILE_PATH)
    df = preprocess_data(df)
    valid_df = validate_data(df)
    save_country_data(valid_df)