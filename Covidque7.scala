package spark.FinalProject

import org.apache.spark.sql.functions.count
import spark.utils.SparkApp

object Covidque7 extends SparkApp with App {

  val phudf = spark.read.json("src/main/resources/covid_data/phu_locations.json")
  //phudf.show(false)

  //question 7:Count all the PHU are temporary closed in each city.
  val phu_temporycloseddf = phudf.where("temporarily_closed == 'Yes'").groupBy("city")
    .agg(count("PHU") as ("count Of Temp_closed")).select("city", "count Of PHU Temp_closed").show()
}
