package sharding

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ListBuffer

object ShaM1 {
  def getRS(spark: SparkSession, df:DataFrame): DataFrame ={
    println("##【ShaM1】getRS")
    // ---------------------------------------
    import spark.implicits._ //隐式转换
    // ---------------------------------------
    val rs = df.groupByKey(r => r.getAs[String]("K") + "," + r.getAs[Int]("S"))
      .flatMapGroups(f = (k, iter) => {
        // ---------------------------------------
        val rsBuffer = new ListBuffer[(Int,Double, Double, Long, Long, Int, String, List[String])]()
        // ---------------------------------------
        val K = k.split(",")(0)
        val S = k.split(",")(1).toInt
        // ---------------------------------------
        val tmpMap: Map[Long, String] = iter.map(r => (r.getAs[Long]("T"), r.getAs[String]("I"))).toMap
        val ts = tmpMap.keySet.toList.sortBy(+_)
        // ---------------------------------------
        val delay = calcDelay(ts)
        var _mean = 0.0
        var _var = 0.0
        // ---------------------------------------
        if (delay.max < 600) {
          _mean = delay.sum.toDouble / delay.size
          val sqrt = delay.map(x => math.pow(x - _mean, 2))
          _var = sqrt.sum / sqrt.size
//          println("##【*】" + K + ">>" + ts + " mean=" + _mean + " var=" + _var)
          // ---------------------------------------
          val st = ts.head
          val et = ts.last
          val Is = tmpMap.values.toList
          val rs1 = (Is.length, _mean, _var, st, et, S, K, Is)
          rsBuffer.append(rs1)
        } else {
          val indices = new ListBuffer[Int]()
          for (i <- delay.indices) {
            if (delay(i) >= 600) indices.append(i)
          }
          indices.append(ts.length - 1)
          var start = 0
          for (i <- indices) {
            val delay_one = delay.slice(start, i)
            if (delay_one.nonEmpty) {
              _mean = delay_one.sum.toDouble / delay_one.size
              val sqrt = delay_one.map(x => math.pow(x - _mean, 2))
              _var = sqrt.sum / sqrt.size
            } else {
              _mean = 0.0
              _var = 0.0
            }
            // ----------------------------------------
            val ts_one = ts.slice(start, i + 1)
            val st_one = ts_one.head
            val et_one = ts_one.last
            val Is_one = for (t <- ts_one) yield tmpMap(t)
//            println("##【*】" + K + ">>" + ts_one + " mean=" + _mean + " var=" + _var)
            val rs1 = (Is_one.length, _mean, _var, st_one, et_one, S, K, Is_one)
            rsBuffer.append(rs1)
            start = i + 1
          }
        }
        // ----------------------------------------
        rsBuffer.toList
      }).toDF("_n", "_mean", "_var", "st", "et", "S", "K", "Is")
    // ----------------------------------------
    rs
  }
  // ----------------------------------------------------------------------
  private def calcDelay(list: List[Long]): List[Long] = {
    list
      .sortBy(+_)
      .sliding(2)
      .map(x => x.last - x.head)
      .toList
  }
  // ----------------------------------------------------------------------


}
