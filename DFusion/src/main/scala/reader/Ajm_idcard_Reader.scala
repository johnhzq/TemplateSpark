package reader

import conf.CommonConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import utils.CommonUtil

object Ajm_idcard_Reader {
  val dataName = "ajm"

  // ----------------------------------------------------------------------
  def getData(spark: SparkSession, datePath: String): DataFrame = {
    import spark.implicits._ //隐式转换
    // ----------------------------------------
    val dataPath = CommonConfig.getBasePath(dataName) + datePath
    val TS = CommonConfig.getTimeSpace(dataName)
    val CItems = CommonConfig.getCItems(dataName)
    val check = TS ++ CItems
    println("##【Ajm_idcard_Reader】" + dataPath)
    // ----------------------------------------
    val df = spark.read.load(dataPath)
    // ----------------------------------------
    df.na.drop(check)
      .map(r => (
        CommonUtil.getTime(r.getAs[String](TS(0))),
        CommonConfig.getSpaceByGbno(r.getAs[String](TS(1))),
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

