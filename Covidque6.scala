package spark.FinalProject


import org.apache.spark.sql.functions._
import spark.utils.SparkApp

object Covidque6 extends SparkApp with App {

  val phudf = spark.read.json("src/main/resources/covid_data/phu_locations.json")
  //phudf.show(false)

  //question 6:List the hours of operation of all the PHU in Toronto. Use below format.
  //PHU,city,friday,monday,operated_by ,saturday,sunday,thursday,tuesday,wednesday
  val phudf_opersationdf = phudf.select("PHU", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
    .where("city == 'Toronto'")
  phudf_opersationdf.na.drop().show()

}
