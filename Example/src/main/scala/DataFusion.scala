import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.desc

import scala.collection.mutable.ListBuffer

object DataFusion {
  def main(args: Array[String]): Unit = {
    val dataList: List[(String, String)] = List(
      ("A1", "B1"),
      ("A1", "B1"),
      ("A1", "C1"),
      ("A1", "C1"),
      ("A1", "D1"),
      ("A1", "D1"),
      ("A1", "B2"),
      ("A1", "C3"),
      ("A1", "D2"),
      ("B1", "C1"),
      ("B1", "C1"),
      ("B1", "D1"),
      ("B1", "D1"),
      ("B1", "C4"),
      ("B1", "D2"),
      ("C1", "D1"),
      ("C1", "D1"),
      ("C1", "D3"))
    // ----------------------------------------------------------------------
    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._ //
    val data = dataList.toDF("type1", "type2")
    // ----------------------------------------------------------------------
    val transactions = dataList.toDF("type1", "type2")
    val patterns = transactions.flatMap(r => {
      val items = r.toSeq.toList
      (1 to items.size) flatMap items.combinations map (xs => xs.map(_.toString))
    })
      .map((_, 1))
      .toDF("K", "V")
//    patterns.foreach(r => println(r.toString()))
    println("【patterns】----------------------------------------------------------------------")
    // ----------------------------------------------------------------------
    val combined = patterns.groupBy(patterns.columns(0))
      .sum()
      .filter(r => r.getAs[Long](1)>1)
      .toDF("K", "V")
//    combined.sort(desc("V")).foreach(r => println(r.toString()))
    println("【combined】----------------------------------------------------------------------")
    // ----------------------------------------------------------------------
    val subpatterns = combined.flatMap(r => {
      val rsBuffer = new ListBuffer[(List[String], (List[String], Long))]()
      val list = r.getAs[Seq[String]]("K").toList
      val frequency = r.getAs[Long]("V")
      rsBuffer += ((list, (Nil, frequency)))
      val sublist = for {
        i <- list.indices
        xs = list.take(i) ++ list.drop(i + 1)
        if xs.nonEmpty
      } yield (xs, (list, frequency))
      rsBuffer ++= sublist
      rsBuffer.toList
    }).toDF("K", "V")
//    subpatterns.foreach(r => println(r.toString()))
    println("【subpatterns】----------------------------------------------------------------------")
    // ----------------------------------------------------------------------
    val rules = subpatterns.groupByKey(r => r.getAs[Seq[String]]("K"))
      .mapGroups((k, iter) => {
        val K = k.toList
        val VBuffer = new ListBuffer[(List[String], Long)]()
        while (iter.hasNext) {
          val r = iter.next().getStruct(1)
          VBuffer.append((r.getAs[Seq[String]](0).toList, r.getLong(1)))
        }
        val V = VBuffer.toList
        (K, V)
      })
      .toDF("K", "V")
//    rules.foreach(r => println(r.toString()))
    println("【rules】----------------------------------------------------------------------")
    // ----------------------------------------------------------------------
    val assocRules = rules.map(r => {
      val _1 = r.getAs[Seq[String]](0).toList
      val VBuffer = new ListBuffer[(List[String], Long)]()
      val iter = r.getAs[Seq[Row]](1).iterator
      while (iter.hasNext) {
        val r = iter.next()
        VBuffer.append((r.getAs[Seq[String]](0).toList, r.getLong(1)))
      }
      val _2 = VBuffer.toList
      val fromCount = _2.find(p => p._1 == Nil).get
      val toList = _2.filter(p => p._1 != Nil)
      if (toList.isEmpty) Nil
      else {
        val result =
          for {
            t2 <- toList
            confidence = t2._2.toDouble / fromCount._2.toDouble
            difference = t2._1 diff _1
          } yield (((_1, difference, confidence)))
        result
      }
    })
    assocRules.foreach(r => println(r.toString()))
    println("【assocRules】----------------------------------------------------------------------")
    // ----------------------------------------------------------------------
    val temp = assocRules.flatMap(r => {
      val rsBuffer = new ListBuffer[(List[String], Double)]()
      for(a <- r){
        val K0 = a._1++a._2
        val K1 = K0.sorted
       if(K0.head==K1.head){
         rsBuffer.append((K0, a._3))
       }else{
         rsBuffer.append((K1, -a._3))
       }
      }
      rsBuffer.toList
    })
      .toDF("K", "V")
      .groupByKey(r => r.getAs[Seq[String]]("K"))
      .mapGroups((k, iter) =>{
        val _1 = k.toList
        var _2 = new Array[Double](2)
        while (iter.hasNext) {
          val confidence = iter.next().getDouble(1)
          if(confidence>0){
            _2(0) = confidence
          }else{
            _2(1) = -confidence
          }
        }
        (_1, _2.toList)
      })
      .toDF("K", "V")
    temp.foreach(r => println(r.toString()))
    println("【-----】----------------------------------------------------------------------")
    // ----------------------------------------------------------------------
    // ----------------------------------------------------------------------
    // ----------------------------------------------------------------------
  }
}
