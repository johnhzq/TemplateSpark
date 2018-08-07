import org.apache.spark.sql.SparkSession

object TopNSecond {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("TopNSecond by Scala")
      .getOrCreate() //隐式转换
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._ //隐式转换
    // ----------------------------------------
    val data = spark.read.text("G:/TopNSecond.txt")
//    val data = spark.read.format("csv")
//      .option("sep", ",")
//      .option("inferSchema", "true") // 自动推测数据类型
//      .load("G:/TopNSecond.txt")
    // ----------------------------------------
    val lines = data.map(r=>(r.toString().split(" ")(0),r.toString().split(" ")(1).toInt))
//    // ----------------------------------------
//    val lines = data.map{ line => (line.split(" ")(0),line.split(" ")(1).toInt) }
//    val groups = lines.groupByKey()
//    val groupsSort = groups.map(tu=>{
//      val key = tu._1
//      val values = tu._2
//      val sortValues = values.toList.sortWith(_>_).take(4)
//      (key,sortValues)
//    })
//    groupsSort.sortBy(tu=>tu._1, ascending = false, 1).collect().foreach(value=>{
//      print(value._1)
//      value._2.foreach(v=>print("\t"+v))
//      println()
//    })
//    sc.stop()
  }

//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("TopNSecond by Scala").setMaster("local")
//    val sc=new SparkContext(conf)
//    val data=sc.textFile("G:/TopNSecond.txt",1)
//    val lines=data.map{ line => (line.split(" ")(0),line.split(" ")(1).toInt) }
//    val groups=lines.groupByKey()
//    val groupsSort=groups.map(tu=>{
//      val key=tu._1
//      val values=tu._2
//      val sortValues=values.toList.sortWith(_>_).take(4)
//      (key,sortValues)
//    })
//    groupsSort.sortBy(tu=>tu._1, ascending = false, 1).collect().foreach(value=>{
//      print(value._1)
//      value._2.foreach(v=>print("\t"+v))
//      println()
//    })
//    sc.stop()
//  }

}
