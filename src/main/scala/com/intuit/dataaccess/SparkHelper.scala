package com.intuit.dataaccess

import org.apache.spark.sql.SparkSession

object SparkHelper {
  def getSparkSession(isLocal: Boolean): SparkSession = {

    val spark = SparkSession.builder.appName("ClickStream")
    if (isLocal) {
      spark.master("local[*]").getOrCreate()
    } else {
      spark.getOrCreate()
    }
  }
}
