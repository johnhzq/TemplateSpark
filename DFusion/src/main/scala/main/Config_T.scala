package main

import conf.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.CommonUtil

object Config_T {
  def main(args: Array[String]): Unit = {
    // ---------------------------------------
    val config = Config()
    // ---------------------------------------
    val spark = SparkSession.builder()
      .master("local")
      .appName("Config_T")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._ //隐式转换
    // ----------------------------------------
    A.getData(spark,"20180624", config).show()
//    A.getData(spark,"20180624", config).map(r => r.toString()).show()
    // ----------------------------------------
    spark.stop()
  }
}

object A {
  val dataName = "ajm"

  def getData(spark: SparkSession, datePath: String, config: Config): DataFrame = {
    import spark.implicits._ //隐式转换
    // ----------------------------------------
    val dataPath = config.getBasePath(dataName) + datePath
    val TS = config.getTimeSpace(dataName)
    val CItems = config.getCItems(dataName)
    val check = TS ++ CItems
    println("##【A】" + dataPath)
    // ----------------------------------------
    val df = spark.read.load(dataPath)
    // ----------------------------------------
    df.na.drop(check)
      .map(r => (
        CommonUtil.getTime(r.getAs[String](TS(0))),
        config.getSpaceByGbno(r.getAs[String](TS(1))),
        r.getAs[String](CItems(0)),
        r.toString()
      ))
      .filter(r => r._2 != (-1))
      .toDF("T", "S", "K", "I")
    // ----------------------------------------

    // ----------------------------------------

  }
}