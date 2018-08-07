import org.apache.spark.sql.SparkSession


object Start {

  def main(args: Array[String]): Unit = {
    // ----------------------------------------------------------------------
    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark SQL basic example")
      //      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    // ----------------------------------------------------------------------
    val dataList: List[(Double, String, Double, Double, String, Double, Double, Double, Double)] = List(
      (0, "male", 37, 10, "no", 3, 18, 7, 4),
      (0, "female", 27, 4, "no", 4, 14, 6, 4),
      (0, "female", 32, 15, "yes", 1, 12, 1, 4),
      (0, "male", 57, 15, "yes", 5, 18, 6, 5),
      (0, "male", 22, 0.75, "no", 2, 17, 6, 3),
      (0, "female", 32, 1.5, "no", 2, 17, 5, 5),
      (0, "female", 22, 0.75, "no", 2, 12, 1, 3),
      (0, "male", 57, 15, "yes", 2, 14, 4, 4),
      (0, "female", 32, 15, "yes", 4, 16, 1, 2),
      (0, "male", 22, 1.5, "no", 4, 14, 4, 5))
    // ----------------------------------------------------------------------
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._ //
    val data = dataList.toDF("affairs", "gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
    //    data.printSchema()
    // ----------------------------------------------------------------------
    // 在Spark-shell中展示，前n条记录
//    data.show(7)
    // 取前n条记录
//    val data2=data.limit(5)
//    data2.show()
    // ----------------------------------------------------------------------
    // 过滤
//    data.filter("age>50 and gender=='male' ").show
    // ----------------------------------------------------------------------
    // 数据框的所有列
//    val columnArray=data.columns
//    println(JSON.toString(columnArray))
    // ----------------------------------------------------------------------
    // 查询某些列的数据
//    data.select("gender", "age", "yearsmarried", "children").show(3)
//    val colArray=Array("gender", "age", "yearsmarried", "children")
//    data.selectExpr(colArray:_*).show(3)
    // ----------------------------------------------------------------------
    // 操作指定的列,并排序
//    data.selectExpr("gender", "age+1","cast(age as bigint)").orderBy($"gender".desc, $"age".asc).show
//    data.selectExpr("gender", "age+1 as age1","cast(age as bigint) as age2").sort($"gender".desc, $"age".asc).show
    // ----------------------------------------------------------------------
//    val data3=data.selectExpr("gender", "age+1 as age1","cast(age as bigint) as age2").sort($"gender".desc, $"age".asc)
    // 查看物理执行计划
//    data3.explain()
    // 查看逻辑和物理执行计划
//    data3.explain(extended=true)
//    Parsed Logical Plan 解析逻辑执行计划
//    Analyzed Logical Plan 分析逻辑执行计划
//    Optimized Logical Plan 优化逻辑执行计划
//    Physical Plan 物理执行计划
    // ----------------------------------------------------------------------
//    data.map(r=>r.toString()).show
//    data.map(r=>r.get(1).toString).show // gender
    // ----------------------------------------------------------------------
//    data.filter("age>50").show // age
//    data.filter(r=>r.getDouble(2)>50).show // age
//    data.filter(r=>r.getString(1).equals("male")).show
//    data.filter(r=>r.getString(1).startsWith("fe")).show
//    data.filter(r=>r.getString(1).equals("male")&&r.getDouble(2)==22).show
    // ----------------------------------------------------------------------
//    var wordcount = data.flatMap(r=>r.toString().replace("[","").replace("]","").split(","))
//      .map((_, 1))
//      .groupBy("_1")
//      .agg(sum("_2"))
//    wordcount = wordcount.withColumnRenamed("_1","word")
//    wordcount = wordcount.withColumnRenamed("sum(_2)","count")
//    wordcount.show()
//    wordcount.write.save("wordCount")
//    wordcount.map(r=>r.toString()).show
//    wordcount.map(r=>r.toString())
//      .write.text("wordCount_txt")
    // ----------------------------------------------------------------------
//    val df = spark.read.load("wordCount")
//    df.show
//    val df = spark.read.text("wordCount_txt")
//    df.show
//    val df = spark.read.load("hdfs://172.16.4.111:8020/user/zhangjun/Data/gongan/ajm/20180624")
    val df = spark.read.load("hdfs://172.16.4.111:8020/user/zhangjun/Data/gongan/ty/20180624")
    df.map(r=>r.toString()).show(5)
//    val df = spark.read.text("hdfs://172.16.4.111:8020/datum/szt/subway/20180624")
//    df.show







  }

}
