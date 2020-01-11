package com.intuit.dataaccess

import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, SparkSession}

object LocalDataReader {
  protected val sparkSession: SparkSession = SparkHelper.getSparkSession(true)

  protected val bucket: String = "samples"

  val timeZones: List[Int] = (0 to 24).toList
  val basePath: String = "/Users/alonsarshalom/IdeaProjects/alon-test/"

  def getActivitiesNumber(client: String, days: Int, accountId: Int, module: String, userId: String): Long = {
    val paths: Set[String] = getActivitiesDataPath(getDates(days), client, accountId, module)
    getDataByUserId(paths, Some(userId))
  }

  def getModulesNumber(client: String, days: Int, accountId: Int, userId: String): Long = {
    val paths: Set[String] = getModulesDataPath(getDates(days), client, accountId)
    getDataByUserId(paths, Some(userId))
  }

  def getUserToAccountNumber(client: String, days: Int, accountId: Int): Long = {
    val paths: Set[String] = getUserToAccountDataPath(getDates(days), client, accountId)
    getDataByUserId(paths, None)
  }

  private def getDataByUserId(paths: Set[String], userId: Option[String]): Long = {
    val filteredPaths = Functions.filterPaths(paths)
    readParquetData(filteredPaths.toList)
      .transform(Functions.filterByUserId(_, userId))
      .transform(Functions.sumByCount)
      .first.getLong(0)
  }

  private def getActivitiesDataPath(days: Set[String], client: String, accountId: Int, module: String): Set[String] = {
    days.map(day => {
      s"$basePath/$bucket/${FolderNames.ACTIVITIES}/${ColumnNames.DATE}=$day/${ColumnNames.CLIENT_ID}=$client/${ColumnNames.ACCOUNT_ID}=$accountId/${ColumnNames.MODULE}=$module"
    })
  }

  private def getModulesDataPath(days: Set[String], client: String, accountId: Int): Set[String] = {
    days.map(day => {
      s"$basePath/$bucket/${FolderNames.MODULES}/${ColumnNames.DATE}=$day/${ColumnNames.CLIENT_ID}=$client/${ColumnNames.ACCOUNT_ID}=$accountId"
    })
  }

  private def getUserToAccountDataPath(days: Set[String], client: String, accountId: Int): Set[String] = {
    days.map(day => {
      s"$basePath/$bucket/${FolderNames.USERS}/${ColumnNames.DATE}=$day/${ColumnNames.CLIENT_ID}=$client/${ColumnNames.ACCOUNT_ID}=$accountId"
    })
  }

  private def getDates(numDays: Int): Set[String] = {
    val day = LocalDate.now()
    Range(0, numDays).map(day.minusDays(_)).map(_.toString).toSet
  }

  private def readParquetData(path: List[String]): DataFrame = {
    if (path.nonEmpty) {
      sparkSession.read.parquet(path: _*)
    } else {
      sparkSession.emptyDataFrame
    }
  }
}
