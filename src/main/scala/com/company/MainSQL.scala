package com.company

import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

/**
 * SQL solution.
 */
object MainSQL {

  def main(args: Array[String]): Unit = {

    val options = new Options

    options.addRequiredOption("csp", "clickStreamPath", true, "Path to click stream data")
    options.addRequiredOption("psp", "purchaseStreamPath", true, "Path to purchase stream data")

    val parser = new DefaultParser
    val cmdArgs = Try(parser.parse(options, args))

    cmdArgs match {

      case Success(cmdArgs) =>
        runCalculations(cmdArgs.getOptionValue("csp"), cmdArgs.getOptionValue("psp"))

      case Failure(exception) =>
        println(exception.getMessage)
    }
  }

  def runCalculations(pathToClickStream: String, pathToPurchaseStream: String): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val purchaseStream = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(pathToPurchaseStream)
    val clickStream = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(pathToClickStream)

    val purchaseAttribution = calculatePurchaseAttribution(spark, purchaseStream, clickStream).cache()
    println("Purchase attribution:")
    purchaseAttribution.show()
    val top10Campaigns = calculateTop10Campaigns(spark, purchaseAttribution)
    println("Top 10 campaigns:")
    top10Campaigns.show()
    val mostPopularChannel = calculateMostPopularChannel(spark, purchaseAttribution)
    println("Most popular channel:")
    mostPopularChannel.show()

    spark.stop()
  }

  def calculatePurchaseAttribution(spark: SparkSession, purchaseStream: DataFrame, clickStream: DataFrame): DataFrame = {
    purchaseStream.createOrReplaceGlobalTempView("purchase_stream")
    clickStream.createOrReplaceGlobalTempView("click_stream")

    spark.sql(
      "with clickstream_with_session_id as ("+
        "select *, sum(case when eventType='app_open' THEN 1 else 0 end) " +
        "over(partition by userid order by eventTime rows between unbounded preceding and current row) as session_id " +
        "from global_temp.click_stream), " +
        "flattened_clickstream as (select " +
        "stream1.userid," +
        "stream1.eventtime," +
        "regexp_extract(stream2.attributes, '((?<=\"campaign_id\": \")[^\"]+)', 1) as campaign_id," +
        "regexp_extract(stream2.attributes, '((?<=\"channel_id\": \")[^\"]+)', 1) as channel_id," +
        "regexp_extract(stream1.attributes,'((?<=\"purchase_id\": \")[^\"]+)', 1) as purchase_id, " +
        "concat(stream1.userid, stream1.session_id) as session_id " +
        "from clickstream_with_session_id stream1 " +
        "join clickstream_with_session_id stream2 on stream1.session_id=stream2.session_id and stream1.userid=stream2.userid " +
        "where stream1.eventType='purchase' " +
        "and stream2.eventType='app_open') " +
        "select userid as user_id, eventtime as purchase_time, campaign_id, purchase_id, session_id, billingCost as billing_cost, isConfirmed as is_confirmed, campaign_id, channel_id " +
        "from flattened_clickstream stream1 join global_temp.purchase_stream stream2 on stream1.eventtime=stream2.purchaseTime and stream1.purchase_id=stream2.purchaseId"
    )
  }

  def calculateTop10Campaigns(spark: SparkSession, purchaseAttribution: DataFrame): DataFrame = {
    purchaseAttribution.createOrReplaceGlobalTempView("purchase_attribution")
    spark.sql(
      "select campaign_id, sum(billing_cost) as total_cost from global_temp.purchase_attribution where is_confirmed=true group by campaign_id order by total_cost desc limit 10"
    )
  }

  def calculateMostPopularChannel(spark: SparkSession, purchaseAttribution: DataFrame): DataFrame = {
    purchaseAttribution.createOrReplaceGlobalTempView("purchase_attribution")
    spark.sql(
      "select channel_id, campaign_id, count(DISTINCT session_id) as session_count from global_temp.purchase_attribution group by channel_id, campaign_id order by session_count desc limit 1"
    )
  }
}
