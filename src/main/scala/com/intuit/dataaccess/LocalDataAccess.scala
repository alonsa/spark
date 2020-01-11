package com.intuit.dataaccess

import org.apache.spark.sql.DataFrame

object LocalDataAccess extends DataAccess {
  override def isLocal: Boolean = true

  val timeZones: List[Int] = (0 to 24).toList
  val basePath: String = "/Users/alonsarshalom/IdeaProjects/alon-test/"

  override def saveLocation(folderName: String): String = s"$basePath/$bucket/$folderName"

  override def readJsonData(paths: Seq[String]): DataFrame = {
    if (paths.nonEmpty) {
      sparkSession.read.json(paths: _*)
    } else {
      sparkSession.emptyDataFrame
    }
  }

  override def getPartitions(dates: Set[String], clients: Set[String]): Seq[String] = {
    val paths = clients.flatMap(client => {
      dates.map(day => {
        s"$basePath/$bucket/${FolderNames.RAW}/$client/$day"
      })
    })
   Functions.filterPaths(paths).toSeq
  }
}
