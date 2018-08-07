package reader

import conf.CommonConfig
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import utils.CommonUtil

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Ap_raw_Reader {
  val dataName = "ap"

  // ----------------------------------------------------------------------
  def getData(spark: SparkSession, datePath: String): DataFrame = {
    import spark.implicits._ //隐式转换
    // ----------------------------------------
    val dataPath = CommonConfig.getBasePath(dataName) + datePath
    val TS = CommonConfig.getTimeSpace(dataName)
    val CItems = CommonConfig.getCItems(dataName)
    val check = TS ++ CItems
    println("##【Ap_raw_Reader】" + dataPath)
    // ----------------------------------------
    val df = spark.read.load(dataPath)
    // ----------------------------------------
    val df1 = df.flatMap(r => {
      val rsBuffer = new ListBuffer[(Long, String, String, String, Long, String, Int, Int)]()
      val _c0 = r.getAs[Long](0)
      val _c1 = r.getAs[String](1)
      val _c2 = r.getAs[String](2)
      val _c3 = r.getAs[String](3)
      val MACs = r.getAs[Seq[Row]](4)
      val it = MACs.iterator
      while (it.hasNext) {
        val one = it.next()
        val _c4 = one.getAs[Long](0)
        val _c5 = one.getAs[String](1)
        val _c6 = one.getAs[Int](2)
        val _c7 = one.getAs[Int](3)
        rsBuffer.append((_c0, _c1, _c2, _c3, _c4, _c5, _c6, _c7))
      }
      rsBuffer.toList
    }).toDF("_c0", "_c1", "_c2", "_c3", "_c4", "_c5", "_c6", "_c7")
    df1.na.drop(check)
      .map(r => (
        r.getAs[Long](TS(0)),
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
