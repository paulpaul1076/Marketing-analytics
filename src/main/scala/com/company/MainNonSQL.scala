package com.company

import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

/**
 * Non-SQL solution.
 */
object MainNonSQL {

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
    val spark = SparkSession.builder().getOrCreate ()

    val purchaseStream = spark.read
      .option ("header", "true")
      .option ("inferSchema", "true")
      .option ("quote", "\"")
      .option ("escape", "\"")
      .csv (pathToPurchaseStream)

    val clickStream = spark.read
      .option ("header", "true")
      .option ("inferSchema", "true")
      .option ("quote", "\"")
      .option ("escape", "\"")
      .csv (pathToClickStream)

    val purchaseAttribution = calculatePurchaseAttribution (spark, purchaseStream, clickStream).cache ()
    println ("Purchase attribution:")
    purchaseAttribution.show ()
    val top10Campaigns = calculateTop10Campaigns (spark, purchaseAttribution)
    println ("Top 10 campaigns:")
    top10Campaigns.show ()
    val mostPopularChannel = calculateMostPopularChannelForEachCampaign (spark, purchaseAttribution)
    println ("Most popular channel:")
    mostPopularChannel.show ()

    spark.stop ()
  }

  def calculatePurchaseAttribution(spark: SparkSession, purchaseStream: DataFrame, clickStream:DataFrame): DataFrame = {
    import spark.implicits._

    val window = Window.partitionBy("userid").orderBy("eventTime").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val clickStreamWithSessionId = clickStream.withColumn("session_id", sum(when(col("eventType") === "app_open", 1).otherwise(0)).over(window))
    val flattenedClickStream =
      clickStreamWithSessionId.as("stream1")
        .join(clickStreamWithSessionId.as("stream2"), Seq("session_id", "userId"))
        .where(($"stream1.eventType" === "purchase").and($"stream2.eventType" === "app_open"))
        .select(
          $"stream1.userid",
          $"stream1.eventtime",
          regexp_extract($"stream2.attributes", "((?<=\"campaign_id\": \")[^\"]+)", 1).as("campaign_id"),
          regexp_extract($"stream2.attributes", "((?<=\"channel_id\": \")[^\"]+)", 1).as("channel_id"),
          regexp_extract($"stream1.attributes", "((?<=\"purchase_id\": \")[^\"]+)", 1).as("purchase_id"),
          concat($"stream1.userid", $"stream1.session_id").as("session_id")
        )
    val purchaseAttribution = flattenedClickStream.as("stream1")
      .join(purchaseStream.as("stream2"), ($"stream1.eventtime" === $"stream2.purchaseTime").and($"stream1.purchase_id" === $"stream2.purchaseId"))
      .select(
        $"userid".as("user_id"),
        $"eventtime".as("purchase_time"),
        $"campaign_id",
        $"purchase_id",
        $"session_id",
        $"billingCost".as("billing_cost"),
        $"isConfirmed".as("is_confirmed"),
        $"campaign_id",
        $"channel_id"
      )

    purchaseAttribution
  }

  def calculateTop10Campaigns(spark: SparkSession, purchaseAttribution: DataFrame): DataFrame = {
    purchaseAttribution.filter(col("is_confirmed") =!= false)
      .groupBy("campaign_id").agg(sum("billing_cost").as("total_cost"))
      .orderBy(col("total_cost").desc)
      .select("campaign_id", "total_cost")
      .limit(10)
  }

  def calculateMostPopularChannelForEachCampaign(spark: SparkSession, purchaseAttribution: DataFrame): DataFrame = {
    purchaseAttribution.groupBy("channel_id", "campaign_id").agg(CountDistinct(col("session_id")).as("session_count"))
      .orderBy(col("session_count").desc)
      .select("channel_id", "campaign_id", "session_count")
      .limit(1)
  }
}
