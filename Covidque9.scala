package spark.FinalProject

import org.apache.spark.sql.functions._
import spark.utils.SparkApp


object Covidque9 extends SparkApp with App {
  // question 9:Find out highest number of resolved cases of covid-19 in each PHU between April 2020 to Sep 2020//RESOLVED_CASES,PHU_NAME,FILE_DATE
  val casesdf = spark.read.option("header", "true").option("inferschema", "true").
    csv("src/main/resources/covid_data/cases_by_status_and_phu.csv")
  //casesdf.createOrReplaceTempView("cases")
  val file_datedf = casesdf
    .selectExpr("PHU_NAME", "RESOLVED_CASES", "FILE_DATE", "substring(FILE_DATE,0,4) as year", "substring(FILE_DATE,5,2) as month")
    .where(col("month").between("04", "09"))
    .where("year == '2020'")
    .groupBy("PHU_NAME")
    .agg(max("RESOLVED_CASES").as("MAX"), min("RESOLVED_CASES").as("MIN"))
    .orderBy(col("MAX").desc)
    .withColumn("HIGHEST RESOLVED", col("MAX") - col("MIN")).as("RESOLVED CASES")
    .drop("MAX", "MIN")
    .show()

}
