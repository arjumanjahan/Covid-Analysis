package spark.FinalProject

import org.apache.spark.sql.functions.{col, count}
import spark.covid.phudf
import spark.utils.SparkApp

object Covidque4 extends SparkApp with App {
  val phudf = spark.read.json("src/main/resources/covid_data/phu_locations.json")
  //phudf.show(false)
  //question 4:Find out number of PHUs are closed in each city on Friday
  val phu_closeddf = phudf.where(col("friday").isNull).groupBy("PHU", "city").count()
    .show()
}
