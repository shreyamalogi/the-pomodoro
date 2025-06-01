from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count, lit

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Retail Store Analysis Single Output") \
    .getOrCreate()

# Path to the dataset in GCS
file_path = "gs://gen2031/retailstore_5mn.csv"

# Load the dataset into a DataFrame
df = spark.read.option("header", "true").csv(file_path)

# Data Preprocessing: Convert columns to appropriate data types
df = df.withColumn("Age", col("Age").cast("int")) \
       .withColumn("Salary", col("Salary").cast("float"))

# 1. Average Salary by Country
df_grouped_by_country = df.groupBy("Country") \
    .agg(avg("Salary").alias("Average_Salary")) \
    .withColumn("Analysis_Type", lit("Average Salary by Country"))

# 2. Gender Distribution
df_grouped_by_gender = df.groupBy("Gender") \
    .agg(count("*").alias("Count")) \
    .withColumn("Analysis_Type", lit("Gender Distribution"))

# 3. Average Age of Customers (single row)
average_age = df.agg(avg("Age").alias("Average_Age"))
average_age_df = average_age.withColumn("Analysis_Type", lit("Average Age of Customers"))

# 4. Total Customers by Country
df_grouped_by_country_customers = df.groupBy("Country") \
    .agg(count("*").alias("Total_Customers")) \
    .withColumn("Analysis_Type", lit("Total Customers by Country"))

# 5. Total Salary Spend by Country
df_grouped_by_country_salary = df.groupBy("Country") \
    .agg(sum("Salary").alias("Total_Salary_Spent")) \
    .withColumn("Analysis_Type", lit("Total Salary Spend by Country"))

# 📌 Make all schemas compatible: add missing columns
def add_missing_columns(df, all_cols):
    for c in all_cols:
        if c not in df.columns:
            df = df.withColumn(c, lit(None))
    return df

# 📌 List all possible columns
all_columns = set()
for temp_df in [df_grouped_by_country, df_grouped_by_gender, average_age_df, df_grouped_by_country_customers, df_grouped_by_country_salary]:
    all_columns.update(temp_df.columns)
all_columns = list(all_columns)

# 📌 Add missing columns to each small DataFrame
dfs = []
for temp_df in [df_grouped_by_country, df_grouped_by_gender, average_age_df, df_grouped_by_country_customers, df_grouped_by_country_salary]:
    temp_df = add_missing_columns(temp_df, all_columns)
    dfs.append(temp_df.select(*all_columns))

# 📌 Merge all DataFrames
final_df = dfs[0]
for df_ in dfs[1:]:
    final_df = final_df.unionByName(df_)

# 📌 Write into a single CSV file
final_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("gs://gen2031/output/final_combined_output")

# Stop the Spark session
spark.stop()
