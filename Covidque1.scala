package spark.FinalProject

import org.apache.spark.sql.functions.col
import spark.utils.SparkApp


object Covidque1 extends SparkApp with App {
  val phudf = spark.read.json("src/main/resources/covid_data/phu_locations.json")
  //phudf.show(false)

  //question 1:list all PHU locations which are allow online appointments. List should not contain columns in French.
  // All French columns have name ending with fr.
  val online_appdf = phudf.where(col("online_appointments").isNotNull)
    .drop("PHU_fr", "additional_information_fr", "address_fr", "location_name_fr", "operated_by_fr")
  online_appdf.show(false)
}
