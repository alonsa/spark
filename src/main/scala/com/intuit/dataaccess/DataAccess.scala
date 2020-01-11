package com.intuit.dataaccess

import java.time.{LocalDate, LocalDateTime}

import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.immutable

trait DataAccess {
  def isLocal: Boolean
  protected val bucket: String = "samples"
  protected val sparkSession: SparkSession = SparkHelper.getSparkSession(isLocal)

  def saveLocation(folderName: String): String
  def getPartitions(dates: Set[String], clients: Set[String]): Seq[String]
  def readJsonData(partitions: Seq[String]): DataFrame

  def getRawDataByDate(clients: Set[String], date: LocalDate, timeOffset: Int): DataFrame = {

    val localDateTimeList: immutable.Seq[LocalDateTime] = Functions.getLocalDateTimess(date, timeOffset)
    val dateList: Set[String] = Functions.getDates(date, timeOffset).map(_.toString).toSet
    val partitions: Seq[String] = getPartitions(dateList, clients)

    readJsonData(partitions.toList)
      .withColumn(ColumnNames.COUNT, typedLit(1))
      .transform(Functions.filterByDateAndTimeOffset(_, localDateTimeList.map(_.toString)))
  }

  def aggregateActivitiesData(data: DataFrame): Unit = {
    val aggregatedList = List(ColumnNames.DATE, ColumnNames.CLIENT_ID, ColumnNames.ACCOUNT_ID, ColumnNames.MODULE, ColumnNames.USER_ID)
    val partitionList = List(ColumnNames.DATE, ColumnNames.CLIENT_ID, ColumnNames.ACCOUNT_ID, ColumnNames.MODULE)
    val transformed = data.transform(Functions.sumupEvents(_, aggregatedList))
    saveParquetData(transformed, partitionList, FolderNames.ACTIVITIES)
  }

  def aggregateModulesData(data: DataFrame): Unit = {
    val aggregatedList = List(ColumnNames.DATE, ColumnNames.CLIENT_ID, ColumnNames.ACCOUNT_ID, ColumnNames.USER_ID)
    val partitionList = List(ColumnNames.DATE, ColumnNames.CLIENT_ID, ColumnNames.ACCOUNT_ID)
    val transformed = data.transform(Functions.sumupEvents(_, aggregatedList)).transform(dropColumns(_, List(ColumnNames.ACTIVITY)))
    saveParquetData(transformed, partitionList, FolderNames.MODULES)
  }

  def aggregateUserToAccountData(data: DataFrame): Unit = {
    val aggregatedList = List(ColumnNames.DATE, ColumnNames.CLIENT_ID, ColumnNames.ACCOUNT_ID)
    val transformed = data.transform(Functions.sumupEvents(_, aggregatedList)).transform(dropColumns(_, List(ColumnNames.ACTIVITY, ColumnNames.MODULE)))
    saveParquetData(transformed, aggregatedList, FolderNames.USERS)
  }

  private def dropColumns(dataFrame: DataFrame, columnsToDrop: List[String]): DataFrame = {
    dataFrame.drop(columnsToDrop: _*)
  }

  private def saveParquetData(data: DataFrame, partitionList: List[String], folderName: String): Unit = {
    data
      .write
      .partitionBy(partitionList: _*)
      .mode(SaveMode.Append)
      .parquet(saveLocation(folderName))
  }
}

object DataAccess {
  def get(isLocal: Boolean): DataAccess = {
    if (isLocal) {
      LocalDataAccess
    } else {
      S3DataAccess
    }
  }
}
