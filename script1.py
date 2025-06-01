from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, desc

# Initialize Spark session
spark = SparkSession.builder.appName("RetailStoreAnalysis").getOrCreate()

# Define GCS file path
file_path = "gs://gen2030/retailstore_5mn.csv"

# Load CSV with error handling
try:
    df = spark.read.option("header", "true").csv(file_path)
except Exception as e:
    print(f"Error reading CSV: {e}")
    spark.stop()
    exit(1)

# Convert necessary columns to correct data types
df = df.withColumn("Salary", col("Salary").cast("float")) \
       .withColumn("Age", col("Age").cast("int")) \
       .withColumn("CustomerID", col("CustomerID").cast("int"))

# Drop nulls to avoid issues during aggregation (optional)
df = df.dropna(subset=["Salary", "Age", "CustomerID", "Country", "Gender"])

# 1. Average Salary by Country
print("Average Salary by Country:")
df_grouped_by_country = df.groupBy("Country").agg(
    avg("Salary").alias("average_salary")
).orderBy(desc("average_salary"))
df_grouped_by_country.show()

df_grouped_by_country.write.mode("overwrite").csv(
    "gs://gen2030/output/average_salary_by_country", header=True
)

# 2. Gender Distribution
print("\nGender Distribution:")
df_grouped_by_gender = df.groupBy("Gender").agg(
    count("CustomerID").alias("count")
).orderBy(desc("count"))
df_grouped_by_gender.show()

df_grouped_by_gender.write.mode("overwrite").csv(
    "gs://gen2030/output/gender_distribution", header=True
)

# 3. Average Age of Customers
print("\nAverage Age of Customers:")
average_age = df.agg(avg("Age").alias("average_age")).collect()
print(f"Average Age: {average_age[0]['average_age']}")

average_age_df = spark.createDataFrame(
    [(average_age[0]['average_age'],)], ["average_age"]
)
average_age_df.write.mode("overwrite").csv(
    "gs://gen2030/output/average_age", header=True
)

# 4. Total Customers by Country
print("\nTotal Customers by Country:")
df_grouped_by_country_customers = df.groupBy("Country").agg(
    count("CustomerID").alias("total_customers")
).orderBy(desc("total_customers"))
df_grouped_by_country_customers.show()

df_grouped_by_country_customers.write.mode("overwrite").csv(
    "gs://gen2030/output/total_customers_by_country", header=True
)

# 5. Top 5 Countries by Total Salary Spend
print("\nTop 5 Countries by Total Salary Spend:")
df_grouped_by_country_salary = df.groupBy("Country").agg(
    sum("Salary").alias("total_salary_spent")
).orderBy(desc("total_salary_spent"))
df_grouped_by_country_salary.show(5)

df_grouped_by_country_salary.write.mode("overwrite").csv(
    "gs://gen2030/output/top_5_countries_by_salary_spend", header=True
)

# Stop Spark session
spark.stop()
