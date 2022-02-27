package io.keepcoding.spark.exercise.streaming

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}

object AntennaStreamingJob extends StreamingJob {

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame = {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
    val antennaMessageSchema: StructType = ScalaReflection.schemaFor[AntennaMessage].dataType.asInstanceOf[StructType]
    dataFrame
      .select(from_json(col("value").cast(StringType), antennaMessageSchema).as("json"))
      .select("json.*")
      .withColumn("timestamp", $"timestamp".cast(TimestampType))
  }

  override def enrichDeviceWithMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def computeBytesPerAntenna(dataFrame: DataFrame): DataFrame =   {
    dataFrame
      .select($"timestamp", $"antenna_id".as("id"), $"bytes")
      .withColumn("type", lit("antenna_total_bytes"))
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"id", window($"timestamp", "5 minutes"), $"type")
      .agg(
        sum("bytes").as("total_bytes"),
      )
      .select($"window.start".as("timestamp"), $"id",  $"total_bytes".as("value"), $"type")
  }

  override def computeBytesPerUser(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"id", $"bytes")
      .withColumn("type", lit("user_total_bytes"))
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"id", window($"timestamp", "5 minutes"), $"type")
      .agg(
        sum("bytes").as("total_bytes"),
      )
      .select($"window.start".as("timestamp"), $"id",  $"total_bytes".as("value"), $"type")
  }

  override def computeBytesPerApp(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .select($"timestamp", $"app".as("id"), $"bytes")
      .withColumn("type", lit("app_total_bytes"))
      .withWatermark("timestamp", "15 seconds")
      .groupBy($"id", window($"timestamp", "5 minutes"), $"type")
      .agg(
        sum("bytes").as("total_bytes"),
      )
      .select($"window.start".as("timestamp"), $"id",  $"total_bytes".as("value"), $"type")
  }
  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .writeStream
      .foreachBatch { (data: DataFrame, batchId: Long) =>
        data
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }
      .start()
      .awaitTermination()
  }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    val columns = dataFrame.columns.map(col).toSeq ++
      Seq(
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour")
      )

    dataFrame
      .select(columns: _*)
      .writeStream
      .partitionBy("year", "month", "day", "hour")
      .format("parquet")
      .option("path", s"${storageRootPath}/data")
      .option("checkpointLocation", s"${storageRootPath}/checkpoint")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = run(args)
}
