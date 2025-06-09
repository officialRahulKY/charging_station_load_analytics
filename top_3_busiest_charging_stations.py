from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, window, count, when, sum, row_number, desc
from pyspark.sql.types import StructType, StructField, TimestampType, StringType
from pyspark.sql.window import Window

import time, os

# configurations
input_path = "/home/rahul/repos/charging_station_load_analytics/input"
output_path = "/home/rahul/repos/charging_station_load_analytics/output"

# schema
schema = StructType(
    [
        StructField("timestamp", TimestampType()),
        StructField("station_id", StringType()),
        StructField("charger_id", StringType()),
        StructField("status", StringType())
    ]
)

# writes filder to output folder
def dump_to_folder(batch_df, batch_id):
    top_3_df = batch_df.withColumn("rank", row_number().over(Window.orderBy(desc(col("utilisation_%"))))).filter(col("rank") < 4)
    top_3_df.write.mode("append").format("json").save(output_path)

    # delete files older than 1 hour in output folder
    now = time.time()
    cutoff = now - (60 * 60)
    deleted_count = 0

    for filename in os.listdir(output_path):
        filepath = os.path.join(output_path, filename)
        if os.path.isfile(filepath):
            fileTime = os.path.getmtime(filepath)
            print(now, cutoff, fileTime) # it wont get printed
            if fileTime < cutoff:
                os.remove(filepath)
                deleted_count += 1

# main spark code
spark = SparkSession.builder.getOrCreate()
raw_df = spark.readStream.format('json').schema(schema).load(input_path)
transformed_df = raw_df.withWatermark("timestamp", "60 minutes").select("*", when(col("status") == "charging", 1).otherwise(0).alias("charging"), when(col("status") == "not_charging", 1).otherwise(0).alias("not_charging"))
agg_df = transformed_df.groupBy(window("timestamp", "30 minutes", "15 minutes"), "station_id").agg(sum("charging").alias("charging_event_count"), sum("not_charging").alias("not_charging_event_count"))
load_summary_df = agg_df.select("*", (col("charging_event_count")/(col("charging_event_count") + col("not_charging_event_count"))*100).alias("utilisation_%")).drop("charging_event_count", "not_charging_event_count")
top_3_stations = load_summary_df.filter(col("utilisation_%") > 85)
write_job = top_3_stations.writeStream.trigger(processingTime = "1 minute").outputMode("update").foreachBatch(dump_to_folder).start(truncate = False) # truncate is only for console
print(f"streaming job has started, please check the output folder at {output_path}")

# stop the job using this -> write_job.stop()
