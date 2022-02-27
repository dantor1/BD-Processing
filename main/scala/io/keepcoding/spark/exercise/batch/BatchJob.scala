package io.keepcoding.spark.exercise.batch

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class Message(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, metric: String, value: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichDeviceWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def computeBytesPerAntenna(dataFrame: DataFrame): DataFrame

  def computeBytesPerUserMail(dataFrame: DataFrame): DataFrame

  def computeBytesPerApplication(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, aggJdbcTable, aggJdbcErrorTable, aggJdbcPercentTable, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    val deviceDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val metadataDF = readAntennaMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val deviceMetadataDF = enrichDeviceWithMetadata(deviceDF, metadataDF).cache()
    val aggAntennaDF = computeBytesPerAntenna(deviceMetadataDF)
    val aggPerUserDF = computeBytesPerUserMail(deviceMetadataDF)
    val aggApplicationDF = computeBytesPerApplication(deviceMetadataDF)


    writeToJdbc(aggAntennaDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggPerUserDF, jdbcUri, aggJdbcPercentTable, jdbcUser, jdbcPassword)
    writeToJdbc(aggApplicationDF, jdbcUri, aggJdbcErrorTable, jdbcUser, jdbcPassword)

    writeToStorage(deviceDF, storagePath)

    spark.close()
  }

}
