package spark.FinalProject

import org.apache.spark.sql.functions.col
import spark.covid.phudf
import spark.utils.SparkApp

object Covidque2 extends SparkApp with App {

  val phudf = spark.read.json("src/main/resources/covid_data/phu_locations.json")
  //phudf.show(false)
  //question 2:List all the PHU locations allow children under 2 in Brampton location.
  val phud_under2df = phudf.where(col("children_under_2").contains("Yes")).where("city == 'Brampton'")
  phud_under2df.show(false)

}
