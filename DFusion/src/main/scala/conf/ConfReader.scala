package conf

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Properties

object ConfReader {
  // ----------------------------------------------------------------------
//  def getDataConf(filePathName: String): Map[String, List[String]] = {
//    println("#【ConfReader】《getDataConf》filePathName = " + filePathName)
  def getDataConf(fileName: String): Map[String, List[String]] = {
    println("#【ConfReader】《getDataConf》fileName = " + fileName)
    // ---------------------------------------
    var dataInfo: Map[String, List[String]] = Map()
    // ---------------------------------------
//    val is = new FileInputStream(new File(filePathName)) // path
////    val reader = new InputStreamReader(is, "UTF-8")
////    val properties = new Properties()
////    properties.load(reader)
////    is.close()
////    reader.close()
    val is = this.getClass.getClassLoader.getResourceAsStream(fileName)
    val reader = new InputStreamReader(is, "UTF-8")
    val properties = new Properties()
    properties.load(reader)
    is.close()
    reader.close()
    // ---------------------------------------
    val en = properties.propertyNames()
    while (en.hasMoreElements) {
      val k = en.nextElement().toString
      val v = properties.getProperty(k)
      val info = v.split("#").toList
//      println(k + ":" + info)
      dataInfo += (k -> info)
    }
    dataInfo // 返回的结果
  }
  // ----------------------------------------------------------------------
//  def getDFConf(filePathName: String): Map[String, Long] = {
//    println("#【ConfReader】《getDFConf》filePathName = " + filePathName)
  def getDFConf(fileName: String): Map[String, Long] = {
    println("#【ConfReader】《getDFConf》fileName = " + fileName)
    // ---------------------------------------
    var dataInfo: Map[String, Long] = Map()
    // ---------------------------------------
//    val is = new FileInputStream(new File(filePathName)) // path
//    val reader = new InputStreamReader(is, "UTF-8")
//    val properties = new Properties()
//    properties.load(reader)
//    is.close()
//    reader.close()
    val is = this.getClass.getClassLoader.getResourceAsStream(fileName)
    val reader = new InputStreamReader(is, "UTF-8")
    val properties = new Properties()
    properties.load(reader)
    is.close()
    reader.close()
    // ---------------------------------------
    val en = properties.propertyNames()
    while (en.hasMoreElements) {
      val k = en.nextElement().toString
      val v = properties.getProperty(k)
//      println(k + ":" + v.toLong)
      dataInfo += (k -> v.toLong)
    }
    dataInfo // 返回的结果
  }
  // ----------------------------------------------------------------------
//  def getSpaceConf(filePathName: String): Map[String, Int] = {
//    println("#【ConfReader】《getSpaceConf》filePathName = " + filePathName)
  def getSpaceConf(fileName: String): Map[String, Int] = {
    println("#【ConfReader】《getSpaceConf》fileName = " + fileName)
    // ---------------------------------------
    var dataInfo: Map[String, Int] = Map()
    // ---------------------------------------
////    val is = new FileInputStream(new File(filePathName)) // path
////    val reader = new InputStreamReader(is, "UTF-8")
////    val properties = new Properties()
////    properties.load(reader)
////    is.close()
////    reader.close()
    val is = this.getClass.getClassLoader.getResourceAsStream(fileName)
    val reader = new InputStreamReader(is, "UTF-8")
    val properties = new Properties()
    properties.load(reader)
    is.close()
    reader.close()
    // ---------------------------------------
    val en = properties.propertyNames()
    while (en.hasMoreElements) {
      val k = en.nextElement().toString
      val v = properties.getProperty(k)
//      println(k + ":" + v.toLong)
      dataInfo += (k -> v.toInt)
    }
    dataInfo // 返回的结果
  }
  // ----------------------------------------------------------------------

  def main(args: Array[String]): Unit = {
//    val info1 = ConfReader.getDataConf("DFusion/conf/dataType.properties")
//    println(info1)
//    val info2 = ConfReader.getDataConf("DFusion/conf/dataInfo.properties")
//    println(info2)
//    val info3 = ConfReader.getDFConf("DFusion/conf/DF.properties")
//    println(info3)
//    val info4 = ConfReader.getSpaceConf("DFusion/conf/gbno_space.properties")
//    println(info4)
//    val info5 = ConfReader.getSpaceConf("DFusion/conf/station_space.properties")
//    println(info5)

    // ---------------------------------------
    println(this.getClass.getClassLoader.getResource("dataInfo.properties").getPath)
    val properties = new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream("dataInfo.properties"))
    println(properties.getProperty("sensordoor"))
  }

}
