package org.brig

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.Serialization
import org.apache.spark.{SparkContext, SparkConf}
import org.json4s._
import org.json4s.jackson.JsonMethods.parse



object JsonReader extends App{

  val spark: SparkSession = SparkSession
    .builder()
    .appName(name = "JsonReader")
    .master(master = "local")
    .getOrCreate()

  val sc = SparkContext.getOrCreate()


 /* Implemented case class for json4s dependencies and implict method for serialization
 Then using sc for extracting json file)
 */
  case class json_obj(
                       id: Option[Int],
                       country: Option[String],
                       points: Option[Int],
                       price: Option[Int],
                       title: Option[String],
                       variety: Option[String],
                       winery: Option[String]
                     )
  implicit val formats = Serialization.formats(ShortTypeHints(List(classOf[json_obj])))



  val json_file = args(0)

  val json_rdd: RDD[String] =  sc.textFile(json_file)


  json_rdd
    .map(parse(_).extract[json_obj])
    .foreach { line =>
      println(line.toString)
    }


  sc.stop()

}



