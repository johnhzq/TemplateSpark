package main

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object WordCount {
  def main(args: Array[String]): Unit = {
    // ---------------------------------------
    if (args.length != 2) {
      System.err.println("【E】 Usage: wordcount <in> <out>")
      System.exit(2)
    }
    // ---------------------------------------
    val inPath = args(0)
    val outPath = args(1)
    // ---------------------------------------
    val spark = SparkSession.builder()
      .master("local")
      .appName("DFusion")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._ //隐式转换
    // ---------------------------------------
    spark.sparkContext.textFile(inPath)
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((x, y) => x + y)
      .sortBy(_._2,ascending = false)
      .foreach(println)
    // ---------------------------------------
    spark.read.text(inPath)
        .flatMap(r => r.toString().replace("[","").replace("]","").split(" "))
        .map(w => (w,1))
        .groupByKey(r => r._1)
        .mapGroups((k, iter) =>{
          (k, iter.toList.length)
        })
        .orderBy(desc("_2"))
        .show()
    // ---------------------------------------
    spark.stop()
  }
}
