
import subprocess
import os
from pyspark.sql import SparkSession

# ---- Configuration ----
GCP_PROJECT = "dbt-crop"
GCP_REGION = "us-central1"
BUCKET_NAME = "dbt-crop-temp-bucket"
BIGQUERY_DATASET = "crop_weather_ds"
BIGQUERY_TABLE = "crop_weather"
CSV_PATH = r"C:\Users\goura\Downloads\Merged_Crop_and_Weather_Data.csv"
BIGQUERY_CONNECTOR_JAR = r"C:\Users\goura\Downloads\spark-3.5-bigquery-0.42.0.jar"
GCS_CONNECTOR_JAR = r"C:\Users\goura\Downloads\gcs-connector-hadoop3-latest.jar"

GSUTIL_PATH = r"C:\Users\goura\AppData\Local\Google\Cloud SDK\google-cloud-sdk\bin\gsutil.cmd"
HADOOP_HOME = r"C:\Users\goura\AppData\Local\spark-3.5.5-bin-hadoop3"
SERVICE_ACCOUNT_KEY_PATH = r"C:\Users\goura\Downloads\dbt-crop-5ccc2f903071.json"  

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_KEY_PATH
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] += os.pathsep + os.path.join(HADOOP_HOME, "bin")

def create_gcs_bucket():
    print(f"🪣 Checking/Creating GCS bucket: {BUCKET_NAME}...")
    try:
        buckets = subprocess.check_output([GSUTIL_PATH, "ls"]).decode("utf-8")
        if f"gs://{BUCKET_NAME}/" in buckets:   	
            print("✅ Bucket already exists.")
        else:
            subprocess.check_call([
                GSUTIL_PATH, "mb", "-p", GCP_PROJECT, "-l", GCP_REGION, f"gs://{BUCKET_NAME}/"
            ])
            print("✅ Bucket created.")
    except Exception as e:
        print(f"❌ Failed to check/create bucket: {e}")
        exit(1)

def load_data_to_bigquery():
    print("🚀 Starting Spark session...")

    spark = SparkSession.builder \
        .appName("CropWeather Spark BigQuery Pipeline") \
        .config("spark.jars", f"{BIGQUERY_CONNECTOR_JAR},{GCS_CONNECTOR_JAR}") \
        .getOrCreate()

    # Hadoop config for GCS
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("google.cloud.auth.service.account.enable", "true")
    hadoop_conf.set("google.cloud.auth.service.account.json.keyfile", SERVICE_ACCOUNT_KEY_PATH)

    print("📥 Reading cleaned CSV...")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(CSV_PATH)
    print(f"✅ Loaded {df.count()} rows and {len(df.columns)} columns.")


    # Rename columns to ensure BigQuery compatibility
    df = df.withColumnRenamed("Crop", "crop") \
           .withColumnRenamed("Crop_Year", "crop_year") \
           .withColumnRenamed("Season", "season") \
           .withColumnRenamed("State", "state") \
           .withColumnRenamed("Area", "area") \
           .withColumnRenamed("Production", "production") \
           .withColumnRenamed("Annual_Rainfall", "annual_rainfall") \
           .withColumnRenamed("Fertilizer", "fertilizer") \
           .withColumnRenamed("Pesticide", "pesticide") \
           .withColumnRenamed("Yield", "yield") \
           .withColumnRenamed("Avg_Temp", "avg_temp") \
           .withColumnRenamed("Min_Temp", "min_temp") \
           .withColumnRenamed("Max_Temp", "max_temp") \
           .withColumnRenamed("Precipitation_(mm)", "precipitation")

    print(f"✅ Loaded {df.count()} rows and {len(df.columns)} columns.")
    df.printSchema()
    df.show(5)
    
    print("📤 Writing to BigQuery...")
    df.write.format("bigquery") \
        .option("table", f"{GCP_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}") \
        .option("temporaryGcsBucket", BUCKET_NAME) \
        .mode("overwrite") \
        .save()

    print("✅ Upload complete.")
    spark.stop()

# ------------------------
# Execute Pipeline
# ------------------------
if __name__ == "__main__":
    create_gcs_bucket()
    load_data_to_bigquery()