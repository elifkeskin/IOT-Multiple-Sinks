import findspark

# pyspark default file is not located on the system path. With this process, pyspark may be added to the system path.

findspark.init("/opt/manual/spark")   # spark'ın bulunduğu dizin


from pyspark.sql import SparkSession, functions as F


spark = SparkSession.builder\
    .appName("iot Multiple Sinks") \
    .master("local[2]") \
    .config("spark.executor.memory", "3g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.7.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark-catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()


# Let's create the iot schema and connection paths (Hive, PostgreSQl, Delta Table).
# Here row_id and created_ts are the columns added by the data generator.

iot_schema = "row_id int, event_ts double, device string, co float, humidity float, light boolean, lpg float, motion boolean, smoke float, temp float, generate_ts timestamp"


jdbcURL = "jdbc:postgresql://localhost/traindb?user=train&password=Ankara06"
deltaPathSensor4d = "hdfs://localhost:9000/user/train/deltaPathSensor4d"


# Let's read the data using the iot schemas we created.
lines = (spark.readStream
          .format("csv")
          .schema(iot_schema)
          .option("header", False)
          .option("maxFilesPerTrigger", 1)
          .load("file:///home/train/data-generator/output")
)

# Let's write the function that filters the data with the specified ids and writes it as a Postgresql, Hive and delta table.
# Data of the sensor with id number 00:0f:00:70:91:0a => postgresql traindb sensor_0a table
# Data of the sensor with id number b8:27:eb:bf:9d:51 => hive test1 sensor_51 table
# Data of the sensor with id number 1c:bf:ce:15:ec:4d => delta table sensor_4d table


def write_to_multiple_sinks(df, batchId):
    df.persist()
    df.show()

    # write postgresql
    df.filter("device= '00:0f:00:70:91:0a'") \
        .write.jdbc(url=jdbcURL,
                    table="sensor_0a",
                    mode="append",
                    properties={"driver":"org.postgresql.Driver"})

    # write to hive
    df.filter("device = '8:27:eb:bf:9d:51'") \
        .write.format("parquet") \
        .mode("append") \
        .saveAsTable("test1.sensor_51")

    # write to delta
    df.filter("device = '1c:bf:ce:15:ec:4d '") \
        .write.format("delta") \
        .mode("append") \
        .save(deltaPathSensor4d)

    df.unpersist()


# Let's create the Checkpoint folder and save its path to the "CheckpointDir" variable.
# Thus, we create a control mechanism and in case of an error anywhere, we can continue the process from where it left off.

checkpointDir = "file///home/train/checkpoint/iot_multiple_sink_foreach_batch"

# start Streaming
streamingQuery = ( lines.writeStream
                   .foreachBatch(write_to_multiple_sinks)
                   .option("checkpointLocation", checkpointDir)
                   .start()
                   )

streamingQuery.awaitTermination()








