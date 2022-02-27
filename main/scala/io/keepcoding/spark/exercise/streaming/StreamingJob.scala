package io.keepcoding.spark.exercise.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(timestamp: Timestamp, id: String, metric: String, value: Long)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def enrichDeviceWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def computeBytesPerAntenna(dataFrame: DataFrame): DataFrame

  def computeBytesPerUser(dataFrame: DataFrame): DataFrame

  def computeBytesPerApp(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    val devicesDF = parserJsonData(kafkaDF)
    val bytesperantenna = computeBytesPerAntenna(devicesDF)
    val bytesperuser = computeBytesPerAntenna(devicesDF)
    val bytesperapp = computeBytesPerApp(devicesDF)
    val FutureAntenna =  writeToJdbc(bytesperantenna, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val FutureUser =  writeToJdbc(bytesperuser, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val FutureApp =  writeToJdbc(bytesperapp, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    val storageFuture = writeToStorage(devicesDF, storagePath)


    Await.result(Future.sequence(Seq(FutureAntenna, FutureUser, FutureApp)), Duration.Inf)

    spark.close()
  }

}
