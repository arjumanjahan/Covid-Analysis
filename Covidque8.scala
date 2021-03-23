package spark.FinalProject

import org.apache.spark.sql.functions.col
import spark.utils.SparkApp

object Covidque8 extends SparkApp with App {

  val phudf = spark.read.json("src/main/resources/covid_data/phu_locations.json")
  //phudf.show(false)

  //phu_locations.json has phone field. Create two more columns.
  // First field should contain only area code and 2nd field should contain extension number.
  val phonedf = phudf.select(col("phone")) //.show(1000)
  val phone_df = phonedf.selectExpr("phone", "substring(phone,0,3) as area_code", "substring(phone,19,4) as extension")
    .where(col("phone").isNotNull) //.filter("phone")
    .show(false)

}
