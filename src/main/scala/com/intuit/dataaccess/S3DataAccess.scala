package com.intuit.dataaccess

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Request
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import scala.collection.mutable

object S3DataAccess extends DataAccess {
  override def isLocal: Boolean = false

  private val rawfolder = "raw-folder"

  override def saveLocation(folderName: String): String = s"s3a://$bucket/$folderName"

  override def readJsonData(partitions: Seq[String]): DataFrame = {
    val paths = partitions.map(p => s"s3a://$p")
    sparkSession.read.json(paths: _*)
  }

  override def getPartitions(dates: Set[String], clients: Set[String]): Seq[String] = {

    val listSupplyFoldersRequest: ListObjectsV2Request = (new ListObjectsV2Request).withBucketName(bucket).withPrefix(rawfolder).withDelimiter("/")
    val amazonS3Client = AmazonS3ClientBuilder.standard().build()

    val listSupplyFolders: mutable.Seq[String] = amazonS3Client.listObjectsV2(listSupplyFoldersRequest).getCommonPrefixes.asScala

    listSupplyFolders
      .filter(path => dates.exists(date => path.contains(date)))
      .filter(path => clients.exists(client => path.contains(client)))
      .map(path => s"$bucket/$rawfolder/$path")
  }
}