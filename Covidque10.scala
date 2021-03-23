package spark.FinalProject

import org.apache.spark.sql.functions.{col, max, sum}
import spark.covid.{casesdf, spark}
import _root_.spark.utils.SparkApp

object Covidque10 extends SparkApp with App {

  val phudf = spark.read.json("src/main/resources/covid_data/phu_locations.json")
  //phudf.show(false)

  val casesdf = spark.read.option("header", "true").option("inferschema", "true").
    csv("src/main/resources/covid_data/cases_by_status_and_phu.csv")
  //casesdf.show(false)

  //question 10: which PHU has more resolved cases of covid-19 than active cases in year 2020.
  val Df = casesdf.select("PHU_NAME", "ACTIVE_CASES", "RESOLVED_CASES", "FILE_DATE").where(col("FILE_DATE").contains("2020"))
  Df.createOrReplaceTempView("cases")

  import spark.sql

  val ResolvedDf = sql(
    """
      |select phu_name,active_cases,resolved_cases,file_date from cases inner join
      |(select max(file_date)as latestdate,(phu_name)as phname from cases group by phu_name)latest
      |on cases.phu_name=latest.phname and cases.file_date=latest.latestdate where resolved_cases > active_cases
      |""".stripMargin
  )
  ResolvedDf.show()

}
