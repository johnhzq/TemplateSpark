package reader

import conf.CommonConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.CommonUtil

object Szt_subway_Reader {
  val dataName = "szt"

  // ----------------------------------------------------------------------
  def getData(spark: SparkSession, datePath: String): DataFrame = {
    import spark.implicits._ //隐式转换
    // ----------------------------------------
    val dataPath = CommonConfig.getBasePath(dataName) + datePath
    val TS = CommonConfig.getTimeSpace(dataName)
    val CItems = CommonConfig.getCItems(dataName)
    val check = TS ++ CItems
    println("##【Szt_subway_Reader】" + dataPath)
    // ----------------------------------------
    val df = spark.read.format("csv")
      .option("sep", ",")
      //.option("inferSchema", "true") // 自动推测数据类型
      .load(dataPath)
    // ----------------------------------------
    df.na.drop(check)
      .filter(r => (r.getAs[String]("_c7")+"").startsWith("I"))
      .map(r => (
        CommonUtil.getTime(r.getAs[String](TS(0)).replace("T", " ").replace(".000Z", "")),
        CommonConfig.getSpaceByStation(r.getAs[String](TS(1))),
        r.getAs[String](CItems(0)),
        r.toString()
      ))
      .filter(r => r._2 != (-1))
      .toDF("T", "S", "K", "I")
    // ----------------------------------------

    // ----------------------------------------
  }
  // ----------------------------------------------------------------------

  // ----------------------------------------------------------------------

}
