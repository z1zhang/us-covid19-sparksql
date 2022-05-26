package com.zhangz1

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Created by zhangz1 on 2022/4/6 19:59
 */
object covidSQL {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建spark配置对象
    //设置Spark的运行环境
    val conf = new SparkConf().setMaster("local[*]").setAppName("covidSQL")
    //创建Spark上下文对象
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val sqlContext = new SQLContext(sc)
    //生成DataFrame对象
    val readDF = sqlContext.read
      .format("csv")
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd")
      .load("src/main/resources/us-counties.csv")
    //数据库配置
    val url = "jdbc:mysql://localhost:3306/uscovid19"
    val prop = new java.util.Properties
    prop.setProperty("user", "uscovid19")
    prop.setProperty("password", "zhang")
    //对数据进行去重
    val distinctDF = readDF.dropDuplicates()
    //对数据缺失值处理
    val fillDF = distinctDF.na.fill(0)
    //对数据异常值处理
    val filterDF = fillDF.filter((fillDF("cases") >= 0).&&(fillDF("deaths") >= 0))
    //将DataFrame的日期格式化
    val covidDF = filterDF.withColumn("date", to_date(to_timestamp(col("date"), "y/M/d")))

    //美国每个州下每个县的累计确诊人数和死亡人数
    val usCountyDF = covidDF.select("state", "county", "date", "cases", "deaths")
      .groupBy("state", "county")
      .agg(sum("cases").as("cases"), sum("deaths").as("deaths"))
      .orderBy("state", "county")
    usCountyDF.write.mode("overwrite").jdbc(url, "us_county", prop)

    //（1）统计美国每日的累计确诊人数和累计死亡人数
    val covidSum = covidDF.groupBy("date").agg(sum("cases").alias("cases"), sum("deaths").alias("deaths"))
      .orderBy("date")
    covidSum.write.mode("overwrite").jdbc(url, "day_cases_deaths", prop)

    //（2）每日的新增确诊人数。新增数=今日数-昨日数
    val last_day_cases = Window.orderBy("date")
    val covidAdd = covidDF.groupBy("date").agg(sum("cases").alias("cases"))
      .withColumn("variation", col("cases") - lag("cases", 1).over(last_day_cases))
      .na.fill(0.toDouble, Seq("variation"))
      .orderBy("date")
    covidAdd.write.mode("overwrite").jdbc(url, "day_add_cases", prop)

    //（3）统计美国每日的新增死亡人数
    val covidAddDeath = covidDF.groupBy("date").agg(sum("deaths").alias("deaths"))
      .withColumn("variation", col("deaths") - lag("deaths", 1).over(last_day_cases))
      .na.fill(0.toDouble, Seq("variation"))
      .orderBy("date")
    covidAddDeath.write.mode("overwrite").jdbc(url, "day_add_deaths", prop)

    //（4）统计截止5.19日，美国每个州的累计确诊人数和死亡人数
    val covidState = covidDF.filter(to_date(to_timestamp(col("date"))) <= lit("2020-05-19"))
      .groupBy("state").agg(sum("cases").alias("cases"), sum("deaths").alias("deaths"))
      .orderBy("cases")
    covidState.write.mode("overwrite").jdbc(url, "state_cases_deaths", prop)

    //（5）统计截止5.19日，美国确诊人数最多的十个州。按确诊人数降序排列，并取前10个州
    val top10Cases = covidState.orderBy(desc("cases"))
      .limit(10)
    top10Cases.write.mode("overwrite").jdbc(url, "top10_cases", prop)

    //（6）统计截止5.19日，美国死亡人数最多的十个州。按死亡人数降序排列，并取前10个州
    val top10Deaths = covidState.orderBy(desc("deaths"))
      .limit(10)
    top10Deaths.write.mode("overwrite").jdbc(url, "top10_deaths", prop)

    //（7）统计截止5.19日，美国确诊人数最少的十个州。按确诊人数升序排列，并取前10个州
    val top10LeastCases = covidState.orderBy(asc("cases"))
      .limit(10)
    top10LeastCases.write.mode("overwrite").jdbc(url, "top10_least_cases", prop)

    //（8）统计截止5.19日，美国死亡人数最少的十个州。按死亡人数升序排列，并取前10个州
    val top10LeastDeaths = covidState.orderBy(asc("deaths"))
      .limit(10)
    top10LeastDeaths.write.mode("overwrite").jdbc(url, "top10_least_deaths", prop)

    //（9）统计截止5.19日，全美和各州的病死率
    val usaRate = covidDF.agg(sum("cases").alias("cases"), sum("deaths").alias("deaths"))
      .withColumn("rate", round(col("deaths") / col("cases") * 100, 2))
      .withColumn("state", lit("USA"))
      .select("state", "cases", "deaths", "rate")

    val deathRate = covidDF
      .groupBy("state")
      .agg(sum("cases").alias("cases"), sum("deaths").alias("deaths"))
      .withColumn("rate", col("deaths") / col("cases") * 100)
      .withColumn("rate", round(col("rate"), 2).alias("rate"))
    //前十病死率
    val top10DeathRate = deathRate.orderBy(desc("rate")).limit(10)
    top10DeathRate.write.mode("overwrite").jdbc(url, "top10_rate", prop)
    //总病死率
    val totalDeathRate = deathRate
      .union(usaRate).orderBy("state")
    totalDeathRate.write.mode("overwrite").jdbc(url, "death_rate", prop)

  }
}