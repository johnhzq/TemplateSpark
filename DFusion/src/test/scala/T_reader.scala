import org.apache.spark.sql.SparkSession
import reader._


object T_reader {
  def main(args: Array[String]): Unit = {
    // ---------------------------------------
    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._ //隐式转换
    // ----------------------------------------
    val readerName = "szt"
    val df2 = ReaderAdmin.getDataFrame(readerName, spark, "20180624")
    df2.printSchema()
    df2.show()
//    df2.sort("K","S","T").show()
//    df2.sort("T","S","K").show()
    // ----------------------------------------
    spark.stop()
  }
}
