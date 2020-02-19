package com.company;

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

class MainTest {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val purchaseStream = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv("src/test/resources/purchaseStream.csv")

    val clickStream = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .csv("src/test/resources/clickStream.csv")

    val purchaseAttributionSQL = MainSQL.calculatePurchaseAttribution(spark, purchaseStream, clickStream)
    val purchaseAttributionNonSQL = MainNonSQL.calculatePurchaseAttribution(spark, purchaseStream, clickStream)

    @Test
    def calculateMostPopularChannel(): Unit = {
        val expectedMostPopularChannel = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("quote", "\"")
          .option("escape", "\"")
          .csv("src/test/resources/mostPopularChannel.csv")
        val mostPopularChannelSQL = MainSQL.calculateMostPopularChannel(spark, purchaseAttributionSQL)
        val mostPopularChannelNonSQL = MainNonSQL.calculateMostPopularChannel(spark, purchaseAttributionNonSQL)
        assert(areDataFramesEqual(expectedMostPopularChannel, mostPopularChannelSQL))
        assert(areDataFramesEqual(expectedMostPopularChannel, mostPopularChannelNonSQL))
    }

    @Test
    def calculatePurchaseAttribution(): Unit = {
        val expectedPurchaseAttribution = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("quote", "\"")
          .option("escape", "\"")
          .csv("src/test/resources/expectedPurchaseAttribution.csv")

        assert(areDataFramesEqual(expectedPurchaseAttribution, purchaseAttributionSQL))
        assert(areDataFramesEqual(expectedPurchaseAttribution, purchaseAttributionNonSQL))
    }

    @Test
    def calculateTop10Campaigns(): Unit = {
        val expectedTop10Campaigns = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .option("quote", "\"")
          .option("escape", "\"")
          .csv("src/test/resources/expectedTop10Campaigns.csv")

        val top10SQL = MainSQL.calculateTop10Campaigns(spark, purchaseAttributionSQL)
        val top10NonSQL = MainNonSQL.calculateTop10Campaigns(spark, purchaseAttributionNonSQL)

        assert(areDataFramesEqual(expectedTop10Campaigns, top10SQL))
        assert(areDataFramesEqual(expectedTop10Campaigns, top10NonSQL))
    }

    private def areDataFramesEqual(df1: DataFrame, df2: DataFrame): Boolean = {
        df1.except(df2).isEmpty && df2.except(df1).isEmpty
    }
}