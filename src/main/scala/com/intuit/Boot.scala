package com.intuit

import com.intuit.dataaccess.DataAccess
import org.apache.spark.sql.DataFrame

object Boot {

  val timeZoneToClients: Map[Int, Set[String]] = Map().withDefaultValue(Set("client1", "client2")) // just for test

  def main(args: Array[String]): Unit = {

    val aggRequest = CliOptions.parseArgs(args)

    val dataAccess = DataAccess.get(aggRequest.isLocal)
    val data: DataFrame = dataAccess.getRawDataByDate(aggRequest.clients, aggRequest.date, aggRequest.timeOffset)
    dataAccess.aggregateActivitiesData(data)
    dataAccess.aggregateModulesData(data)
    dataAccess.aggregateUserToAccountData(data)
    //    val activities = LocalDataReader.getActivitiesNumber(aggRequest.clients.head, 7, 1, "module_1", "user1")
    //    val modules = LocalDataReader.getModulesNumber(aggRequest.clients.head, 7, 1, "user1")
    //    val users = LocalDataReader.getUserToAccountNumber(aggRequest.clients.head, 7, 1)
  }

}
