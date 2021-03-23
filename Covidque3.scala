package spark.FinalProject

import org.apache.spark.sql.functions.col
import spark.utils.SparkApp


object Covidque3 extends SparkApp with App {
  val phudf = spark.read.json("src/main/resources/covid_data/phu_locations.json")
  phudf.show(false)

  //question 3:List all the PHU locations in Toronto has free parking with drive through.

  val free_parkingdf = phudf.where("city == 'Toronto'").where("free_parking == 'Yes'")
  free_parkingdf.show()

}
