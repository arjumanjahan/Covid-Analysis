package spark.FinalProject

import org.apache.spark.sql.functions._
import spark.covid.{casesdf, spark}
import _root_.spark.utils.SparkApp

object Covidque13 extends SparkApp with App {

  val casesdf = spark.read.option("header", "true").option("inferschema", "true").
    csv("src/main/resources/covid_data/cases_by_status_and_phu.csv")

  //question 13:Find out on which date highest and lowest death reported in “NIAGARA REGION” PHU.

  val lowdf = casesdf.select("FILE_DATE", "PHU_NAME", "DEATHS").where("PHU_NAME == 'NIAGARA REGION'")
    .groupBy("FILE_DATE").agg(min("DEATHS") as "MINDEATHS").orderBy("MINDEATHS").limit(1)

  val highdf = casesdf.select("FILE_DATE", "PHU_NAME", "DEATHS").where("PHU_NAME == 'NIAGARA REGION'")
    .groupBy("FILE_DATE").agg(max("DEATHS") as "MAXDEATHS").orderBy(org.apache.spark.sql.functions.col("MAXDEATHS").desc).limit(1)

  val mindf = lowdf.select("FILE_DATE", "MINDEATHS").toDF("Date", "DEATHS") //.show
  val maxdf = highdf.select("FILE_DATE", "MAXDEATHS").toDF("Date_Max", "MAX DEATHS") //.show

  mindf.createOrReplaceTempView("MIN")
  maxdf.createOrReplaceTempView("MAX")

  spark.sql("select * from MIN UNION select * from MAX").show()

//  val lowdf = casesdf.select("FILE_DATE", "PHU_NAME", "DEATHS").where("PHU_NAME == 'NIAGARA REGION'")
//    .groupBy("FILE_DATE").agg(min("DEATHS") as "MINDEATHS",max("DEATHS") as "MAXDEATHS")
//    .orderBy(col("MAXDEATHS").desc).limit(1).union((col("MINDEATHS")))
//    .show()
}
