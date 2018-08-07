import java.io.{File, FileInputStream, InputStream, InputStreamReader}
import java.util
import java.util.Properties

import conf.{CommonConfig, ConfReader}
import org.apache.spark.sql.SparkSession
import reader.Ajm_idcard_Reader

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object Test {

  def main(args: Array[String]): Unit = {
    // ----------------------------------------------------------------------
//        val spark = SparkSession.builder()
//          .master("local")
//          .appName("Spark SQL basic example")
//          //      .config("spark.some.config.option", "some-value")
//          .getOrCreate()
//        val df = Ajm_idcard_Reader.getData(spark, "20180624")
//        df.show()
//        println(df.count())
    // ----------------------------------------------------------------------
//    // -----【方式一】-----
//    // # 这种方式固定从src/main/resources获取配置文件
//    // # 打包时一并放入jar包中，修改较为麻烦
//    val ipstream = this.getClass.getResourceAsStream("conf/dataInfo.properties")
//    val properties1 = new Properties()
//    properties1.load(ipstream)
//    println(properties1.getProperty("ajm_idcard"))
//    // -----【方式二】-----
//    // # 设定为jar包的绝对路径
//    // # 在IDE中运行时为project的绝对路径
//    // # System.getProperty("user.dir")的方式较为固定，只能获取当前路径
//    val filePath2 = System.getProperty("user.dir")+"/DFusion/conf/"
//    val is = new FileInputStream(filePath2+"dataInfo_L.properties")
//    // 转换流：将字节输入输出流转换成字符输入输出流，解决中文乱码
//    val reader = new InputStreamReader(is, "UTF-8")
//    val properties2 = new Properties()
//    properties2.load(reader)
//    println(properties2.getProperty("ajm_idcard"))
//    // -----【方式三】-----
//    val properties3 = new Properties()
//    properties3.load(new InputStreamReader(new FileInputStream("DFusion/conf/dataInfo_L.properties"), "UTF-8"))
//    println(properties3.getProperty("ajm_idcard"))
//    // ----------------------------------------------------------------------
//    println( this.getClass.getResource("conf/dataInfo.properties"))
//    println( this.getClass.getResource("conf/dataInfo.properties").getPath)
    // ----------------------------------------------------------------------
//    val datainfo = ConfReader.getDataConf("DFusion/conf/dataInfo.properties")
//    val di = datainfo.get("ajm").get
//    println(di)
//    for (x <- di) {
//      println(x)
//    }
//    println(di.take(1))
//    println(di(1))
    // ----------------------------------------------------------------------
//    println(CommonConfig.getThVsWindow)
//    val t = 1529833478L
//    val t1 = t/CommonConfig.getWindow
//    var t2 = 0L
//    val x = t*1.0/CommonConfig.getWindow - t1
//    println(x)
//    if(x<CommonConfig.getThVsWindow){
//      t2 = (t1-1)*(-10)-0 // L
//    }
//    else if(x>(1.0-CommonConfig.getThVsWindow)){
//      t2 = (t1+1)*(-10)-1 // R
//    }
//    println(t1)
//    println(t2)
//    t2 match {
//      case 0 => println(1)
//      case _ => println(2)
//    }






    // ----------------------------------------------------------------------
//    val tmp = Array(1.0).toList
//    println(tmp)
//    println("simpleMovingAverage = "+simpleMovingAverage(tmp,2))
//    println("calcDelay1 = "+calcDelay1(tmp))
//    println("calcDelay2 = "+calcDelay2(tmp))
//    println("calcDelay3 = "+calcDelay3(tmp))
//    println(tmp.sortBy(+_).sliding(2).map(x => ("x",x.toString())).toMap)
//    val mean = tmp.sum/tmp.size
//    println(tmp.map(x=>math.sqrt(x-mean)))
    // ----------------------------------------------------------------------
    val ts = Array(1,2,4,8,9,10,16,17,18,29,38).toList
    var tmpMap2: Map[String, List[(Int,String)]] = Map()
    val ab = new ArrayBuffer[(Int,String)]()
    ab.append((1,"11"))
    ab.append((2,"22"))
    ab.append((3,"33"))
    tmpMap2 += ((1+"_"+3) -> ab.toList)
    val  TIs = tmpMap2("1_3")
    println(TIs.map(TI => TI._1))
    println(TIs.map(TI => TI._2))
//    val ts = Array(1,10,16,29,38).toList
//    println(Array(1,10,16,38,29).toList.sortBy(+_))
//    val test = ts.map(r=>(r,"_"+r)).toMap
//    println(test)
//    println(test(9))
//    M0(ts)
//    println("###########################")
//    M1(ts)

  }
  def M0(ts: List[Int])={
    var tmpMap2: Map[String, List[Int]] = Map()
    var t_head = ts.head
    var t_tail = ts.head
    val ab = new ArrayBuffer[Int]()
    for (t_p <- ts) {
      if (Math.abs(t_p - t_tail) < 4) {
        ab.append(t_p)
        t_tail = t_p
      } else {
        tmpMap2 += ((t_head+"_"+t_tail) -> ab.toList)
        ab.clear()
        t_head = t_p
        ab.append(t_p)
        t_tail = t_p
      }
    }
    tmpMap2 += ((t_head+"_"+t_tail) -> ab.toList)
    // ----------------------------------------
    for (kk <- tmpMap2.keySet) {
      val Is = tmpMap2(kk)
      // ----------------------------------------
      val times = Is.sortBy(+_)
      val delay = calcDelay(times)
      val _mean = delay.sum.toDouble/delay.size
      val sqrt = delay.map(x => math.pow(x-_mean,2))
      val _var = sqrt.sum/sqrt.size
      println("##【M0】"+Is+">>"+kk+" mean="+_mean+" var="+_var)
      // ----------------------------------------
    }
  }
  def calcDelay(list: List[Int]): List[Int] = {
    list
      .sortBy(+_)
      .sliding(2)
      .map(x => x.last - x.head)
      .toList
  }

  def M1(ts: List[Int])={
    val delay = calcDelay(ts)
    println(delay.max)
    var indices = new ListBuffer[Int]()
    for(i <- delay.indices){
      if(delay(i)>=4) indices.append(i)
    }
    indices.append(ts.length-1)
//    println(indices.toList)
//    println("------------------------")
    var _mean = 0.0
    var _var = 0.0
    var start = 0
    println(indices)
    for(i <- indices){
      val delay_one = delay.slice(start,i)
      println("delay.slice("+start+","+i+")="+delay_one)
      if(delay_one.nonEmpty){
        _mean = delay_one.sum.toDouble/delay_one.size.toDouble
        val sqrt = delay_one.map(x=>math.pow(x-_mean,2))
        _var = sqrt.sum/sqrt.size
      }else{
        _mean = 0.0
        _var = 0.0
      }
      // ----------------------------------------
      val ts_one = ts.slice(start,i+1)
      println("ts.slice("+start+","+(i+1)+")="+ts_one)
      val st_one = ts_one.head
      val et_one = ts_one.last
      println("##【M1】"+ts_one+">>"+st_one+"_"+et_one+" mean="+_mean+" var="+_var)
      start = i+1
    }
  }


  def simpleMovingAverage(values: List[Double], period: Int): List[Double] = {
    (for (i <- 1 to values.length) yield if (i<period) 0.00 else values.slice(i - period, i).sum/period).toList
  }

  def calcDelay1(list: List[Double]): List[Double] = {
    list
      .sortBy(+_)
      .sliding(2)
      .map(x => x.last - x.head)
      .toList
  }
  @tailrec
  def calcDelay2(list: List[Double], acc: List[Double] = List.empty): List[Double] = {
    list.sortBy(+_) match {
      case head :: tail if tail != Nil =>
        val newAcc = acc ++ List(tail.head - head)
        val nextList = tail
        calcDelay2(nextList, newAcc)
      case head :: tail if tail == Nil => acc
    }
  }
  def calcDelay3(list: List[Double]): List[Double] = {
    val result = list.sortBy(+_)
    (for (i <- Range(result.length-1, 0, -1)) yield result(i) - result(i-1)).toList.reverse
  }

}
