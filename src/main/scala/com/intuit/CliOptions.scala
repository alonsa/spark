package com.intuit

import java.time.LocalDate

case class AggRequest(isLocal: Boolean, clients: Set[String], date: LocalDate, timeOffset: Int)

object CliOptions {

  /**
   *
   * @param args - args by the following order:
   *             1. clients - all the client ids to query (must have the same time offset)
   *             3. timeOffset - the time offset for all the clients
   *             2. date - the date to aggregate (optional - default yesterday)
   */
  def parseArgs(args: Array[String]): AggRequest = {
    val lift = args.lift
    if (lift(0).isEmpty || lift(1).isEmpty) {
      throw new IllegalArgumentException("Missing client list argument")
    }

    val clients = lift(0).get.split(",").toSet
    val timeOffset = lift(1).get.toInt

    val date: LocalDate = lift(2)
      .map(str => LocalDate.parse(str))
      .getOrElse(LocalDate.now().minusDays(1))

    AggRequest(isLocal, clients, date, timeOffset)
  }

  private def isLocal: Boolean = {
    Option(System.getenv("IS_LOCAL")).exists(_.toBoolean)
  }
}



