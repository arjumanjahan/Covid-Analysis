package spark.FinalProject

import org.apache.spark.sql.functions._
import spark.utils.SparkApp


object Covidque12 extends SparkApp with App {
  //question: 12:Find out total active cases remaining in each PHU//PHU_NAME,ACTIVE_CASES
  val casesdf = spark.read.option("header", "true").option("inferschema", "true").
    csv("src/main/resources/covid_data/cases_by_status_and_phu.csv")

//val totalactiveDf= casesdf.select(col("PHU_NAME"),col("ACTIVE_CASES"),col("FILE_DATE"))
//  .groupBy("PHU_NAME","ACTIVE_CASES").agg(max("FILE_DATE")as "max").orderBy(desc("max"))
//  .show()
    casesdf.createOrReplaceTempView("cases")

  import spark.sql

  val totalactivedf = sql(
    """
      |select phu_name,active_cases,file_date from cases inner join
      |(select max(file_date)as latestdate,phu_name as name from cases group by phu_name)submax
      | on cases.file_date=submax.latestdate and cases.phu_name=submax.name
      |""".stripMargin
  )
  totalactivedf.show()


}