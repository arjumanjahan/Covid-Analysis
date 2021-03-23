package spark.FinalProject

import org.apache.spark.sql.functions.{col, split}
import spark.utils.SparkApp

object Covidque5 extends SparkApp with App {

  val phudf = spark.read.json("src/main/resources/covid_data/phu_locations.json")
  //phudf.show(false)
  //question 5:find out all the PHU locations which are open during 20:00 and 21:00 on Monday in Toronto city.
  //monday,09:00-13:00,city

  val mondaydf = phudf.withColumn("col1", split(col("monday"), "-").getItem(0))
    .withColumn("col2", split(col("monday"), "-").getItem(1))
  val findf = mondaydf.withColumn("col3", split(col("col1"), ":").getItem(0))
    .withColumn("col4", split(col("col1"), ":").getItem(1))
    .withColumn("col5", split(col("col2"), ":").getItem(0))
    .withColumn("col6", split(col("col1"), ":").getItem(1))
  val mondf = findf.where("city == 'Toronto'").where("col3 <='20' and col5 >'20' or col5 == '00'")
    .drop("col1", "col2", "col3", "col4", "col5", "col6")
    .select("PHU", "monday", "city")
    .show()

}
