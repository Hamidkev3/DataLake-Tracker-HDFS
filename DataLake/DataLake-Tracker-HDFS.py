# DataLake-Tracker-HDFS
import datetime
import jdatetime
import oracledb
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, expr, col, to_timestamp, current_timestamp, lit
from pyspark.sql.types import StringType


def convert_to_shamsi(date):
    """Converts Gregorian date to Shamsi (Jalali) date."""
    jalali_date = jdatetime.date.fromgregorian(
        year=date.year, month=date.month, day=date.day)
    return jalali_date.strftime("%Y-%m-%d")


def get_first_day_of_month():
    """Returns the first day of the current Shamsi month in Gregorian date."""
    yesterday_date = datetime.date.today() - datetime.timedelta(days=1)
    jalali_date = jdatetime.date.fromgregorian(year=yesterday_date.year,
                                               month=yesterday_date.month, day=yesterday_date.day)
    first_day = str(jalali_date.replace(day=1))
    first_day_shamsi_in_gregorian = jdatetime.date(int(first_day[0:4]),
                                                   int(first_day[5:7]), int(first_day[8:10])).togregorian()
    return first_day_shamsi_in_gregorian


def get_yesterday_month_id():
    """Returns yesterday's date in Shamsi Year-Month format."""
    yesterday_date = datetime.date.today() - datetime.timedelta(days=1)
    jalali_date = jdatetime.date.fromgregorian(year=yesterday_date.year,
                                               month=yesterday_date.month, day=yesterday_date.day)
    jalali_date_pattern = str(jalali_date.strftime("%Y-%m-%d"))
    return jalali_date_pattern[0:7]


def get_yesterday():
    """Returns yesterday's date in Gregorian format."""
    yesterday_date = datetime.date.today() - datetime.timedelta(days=1)
    return yesterday_date.strftime("%Y-%m-%d")


def configure_spark():
    """Configures and returns Spark session."""
    return SparkSession.builder.master(
        "spark://sparkmaster.sample.com:7077").config("spark.executor.memory", "16g"
                                                      ).getOrCreate()


def read_table_from_database(spark, url, user, password, driver, query):
    """Reads a table from the database using PySpark."""
    connection_details = {"user": user, "password": password, "driver": driver}
    return spark.read.jdbc(url=url, properties=connection_details, table=query)


def write_lineage_data_to_database(df, url, user, password, driver, dbtable):
    """Writes lineage data to the database."""
    df.write.format("jdbc") \
        .mode("append")\
        .option("url", url) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .option("dbtable", dbtable).save()


def write_updated_df_to_hdfs(df, output_path):
    """Writes data into HDFS cluster."""
    df.write.format("parquet") \
        .mode("append") \
        .save(output_path)

# Cutoff table contains: CUTOFF_TIME (time when data is updated up to), CUTOFF_ID (max id in the dataset), TABLE_NAME (dataset name)
# Lineage table contains: LINEAGE_KEY (Incremental), DATA_LOAD_STARTED (time when process of reading the dataset started),
# TABLE_NAME (dataset name), WAS_SUCCESSFUL (writing data into HDFS was successful or not),
# DATA_LOAD_COMPLETED (time when writing the dataset completed), SOURCE_SYSTEM_CUTOFF_TIME (CUTOFF_TIME), ROW_NO (row counts)

def update_cutoff_table(connection, ct_cutoff, max_id_data, tablename):
    """Updates Cutoff table with max id in the dataset."""
    with connection.cursor() as cursor:
        cursor.execute("UPDATE bi_staging.log_cutoff SET CUTOFF_TIME = :ct_cutoff, CUTOFF_ID = :max_id_data WHERE TABLE_NAME = :tablename",
                       (ct_cutoff, max_id_data, tablename))
        connection.commit()


def update_lineage_table(connection, ct_load_comp, ct_cutoff, row_counts, max_lineage_key):
    """Updates Lineage table with dataset row counts."""
    with connection.cursor() as cursor:
        cursor.execute("UPDATE bi_staging.log_lineage SET WAS_SUCCESSFUL = 1, DATA_LOAD_COMPLETED = :ct_load_comp, "
                       "SOURCE_SYSTEM_CUTOFF_TIME = :ct_cutoff, ROW_NO = :row_counts WHERE LINEAGE_KEY = :max_lineage_key",
                       (ct_load_comp, ct_cutoff, row_counts, max_lineage_key)
                       )
        connection.commit()


def main():
    try:
        # Spark configuration and connection details
        spark = configure_spark()
        # Properties
        url = "jdbc:oracle:thin:@192.168.11.11:1521/oracle_db_url"
        user = "sample"
        password = "sample"
        driver = "oracle.jdbc.driver.OracleDriver"
        sid = "192.168.11.11:1521/oracle_db_url"

        # Read the Cutoff and Lineage tables using PySpark
        lineage_table = "log_lineage_table"
        df_lineage = read_table_from_database(
            spark, url, user, password, driver, lineage_table)

        # Write a new row in Lineage table with pyspark
        tablename = 'YourArbitraryTable'
        df_lineage = df_lineage.drop("LINEAGE_KEY")
        df_lineage = df_lineage.withColumn(
            "DATA_LOAD_STARTED", current_timestamp())
        df_lineage = df_lineage.withColumn("TABLE_NAME", lit(tablename))
        # Wrtie the dataset with Lineage key column isn't completed yet
        df_lineage = df_lineage.withColumn(
            "DATA_LOAD_COMPLETED", lit(None).cast(StringType()))
        df_lineage = df_lineage.withColumn("WAS_SUCCESSFUL", lit(0))
        df_lineage = df_lineage.withColumn(
            "SOURCE_SYSTEM_CUTOFF_TIME", lit(None).cast(StringType()))
        df_lineage = df_lineage.withColumn(
            "ROW_NO", lit(None).cast(StringType()))

        # Write lineage data to the database
        write_lineage_data_to_database(
            df_lineage, url, user, password, driver, lineage_table)

        # Query to fetch dataset out of an SQL server database
        yesterday_id = get_yesterday()
        first_day_of_month_id = get_first_day_of_month()
        query = f"(SELECT Id, PaymentDate, Amount, Status FROM sampledataset WHERE CAST(PaymentDate AS DATE) BETWEEN '{first_day_of_month_id}' AND '{yesterday_id}') AS subq"

        df = read_table_from_database(
            spark, url, user, password, driver, query)

        # Perform date conversion and extract month ID
        convert_to_shamsi_udf = udf(convert_to_shamsi, StringType())
        df = df.withColumn("CreateDate", to_timestamp(col("CreateDate")))
        df = df.withColumn(
            "ShamsiDate", convert_to_shamsi_udf(df["CreateDate"]))
        df = df.withColumn("MonthID", expr("substring(ShamsiDate, 1, 7)"))

        # Fetch lineage_table again with the new row added above
        df_max_lineage_key = read_table_from_database(
            spark, url, user, password, driver, lineage_table)

        # Fetch max lineage key from the database
        df_max_lineage_key_select = df_max_lineage_key.filter(
            df_max_lineage_key.TABLE_NAME == tablename)
        # Add a column with Lineage key to the dataset
        max_lineage_key = df_max_lineage_key_select.select(
            expr("max(LINEAGE_KEY)")).collect()[0][0]

        # Write max lineage key to the dataset
        df = df.withColumn("LINEAGE_KEY", lit(max_lineage_key))

        # Path for datalake in current month folder in the HDFS cluster
        current_month_id = get_yesterday_month_id()
        output_path = f"hdfs://namenode.sample.com:9000/sampledataset/MonthID={current_month_id}"

        # Write dataset with Lineage key column into HDFS cluster
        write_updated_df_to_hdfs(df, output_path)

        # Updating Cutoff table with max id in the dataset and Lineage table with dataset row counts
        ct_load_comp = datetime.datetime.now()
        max_id_data = df.select(expr("max(id)")).collect()[0][0]
        row_counts = df.count()
        # Dataset updated until the start of today (midnight)
        ct_cutoff = datetime.datetime.today().replace(
            hour=0, minute=0, second=0, microsecond=0)
        # Conection to Oracle database
        cs = f"{user}/{password}@{sid}"
        connection = oracledb.connect(cs)

        # Update log_cutoff and log_lineage tables with update queries in the database(oracle)
        update_cutoff_table(connection, ct_cutoff, max_id_data, tablename)
        update_lineage_table(connection, ct_load_comp,
                             ct_cutoff, row_counts, max_lineage_key)

        spark.stop()

    except Exception as e:
        print(f"An error occurred: {e}")
        spark.stop()


if __name__ == "__main__":
    main()
