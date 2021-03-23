package spark.FinalProject

import org.apache.spark.sql.functions._
import _root_.spark.covid.casesdf
import spark.utils.SparkApp


object Covidque14 extends SparkApp with App {

  //wrong :question 14:Find out total resolved cases of covid-19 in “NORTH BAY PARRY SOUND DISTRICT” in month May 2020 and Oct 2020.

  val casesdf = spark.read.option("header", "true").option("inferschema", "true").
    csv("src/main/resources/covid_data/cases_by_status_and_phu.csv")

  val file_datedf = casesdf
    .selectExpr("PHU_NAME","FILE_DATE as Date", "RESOLVED_CASES","substring(FILE_DATE,0,4) as Year", "substring(FILE_DATE,5,2) as Month","substring(FILE_DATE,7,2) as Day")
    .where("Month== '05' or Month =='10'")
    .where("Year == '2020'")
    .where("PHU_NAME == 'NORTH BAY PARRY SOUND DISTRICT'")
    .groupBy("Month").agg(sum("RESOLVED_CASES") as "MAXResolved").orderBy("MAXResolved")
    .show()

}
