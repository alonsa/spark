package com.intuit.dataaccess

import java.time.{LocalDate, LocalDateTime}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.immutable
import scala.reflect.io.Path

object Functions {
  def filterByUserId(dataFrame: DataFrame, maybeUserId: Option[String]): DataFrame = {
    import dataFrame.sqlContext.implicits._
    maybeUserId match {
      case Some(userId) => dataFrame.filter($"${ColumnNames.USER_ID}" === userId)
      case None => dataFrame
    }
  }

  def sumByCount(dataFrame: DataFrame): DataFrame = {
    dataFrame.select(ColumnNames.COUNT).agg(sum(ColumnNames.COUNT))
  }

  def filterByDateAndTimeOffset(dataFrame: DataFrame, dates: Seq[String]): DataFrame = {
    import dataFrame.sqlContext.implicits._

    val dateToTimeStamp: UserDefinedFunction = udf((fileName: String, date: String) => {
      val hour = fileName.takeRight(10).take(2).toInt
      LocalDate.parse(date).atStartOfDay().plusHours(hour).toString
    })

    val toDate: UserDefinedFunction = udf((fileName: String) => {
      val date = fileName.takeRight(21).take(10)
      LocalDate.parse(date).toString
    })

    dataFrame
      .withColumn("fileName", input_file_name)
      .withColumn(ColumnNames.DATE, toDate($"fileName"))
      .withColumn("FULL_DATE", dateToTimeStamp($"fileName", $"date"))
      .filter($"FULL_DATE".isin(dates: _*))
      .drop($"FULL_DATE")
      .drop($"fileName")
  }

  def filterPaths(paths: Set[String]): Set[String] = {
    paths.filter(path => Path(path).exists)
  }

  def sumupEvents(dataFrame: DataFrame, groupColumns: List[String]): DataFrame = {
    val sumCount = "sum(count)"

    groupColumns match {
      case Nil => dataFrame
      case head :: tail =>
        dataFrame.groupBy(head, tail: _*)
          .sum(ColumnNames.COUNT)
          .withColumnRenamed(sumCount, ColumnNames.COUNT)
    }
  }

  def getLocalDateTimess(date: LocalDate, timeOffset: Int): immutable.Seq[LocalDateTime] = {
    val localDateTime: LocalDateTime = date.atStartOfDay().plusHours(timeOffset)
    val endDate = localDateTime.plusDays(1)
    (Iterator.iterate(localDateTime)(_ plusHours 1) takeWhile (_ isBefore endDate)).toList
  }

  def getDates(date: LocalDate, timeOffset: Int): Seq[LocalDate] = getLocalDateTimess(date, timeOffset).map(_.toLocalDate).toSet.toList
}
