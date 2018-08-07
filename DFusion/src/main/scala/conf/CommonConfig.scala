package conf

import scala.collection.mutable.ListBuffer
import util.control.Breaks.{break, breakable}

object CommonConfig {
  // ----------------------------------------------------------------------
//    private var th_window: Map[String, Long] = _
//  private var dataType: Map[String, List[String]] = _
//  private var dataInfo: Map[String, List[String]] = _
//  private var gbno_space: Map[String, Int] = _
//  private var station_space: Map[String, Int] = _
//
//  def init(th_window: Map[String, Long], dataType: Map[String, List[String]], dataInfo: Map[String, List[String]],gbno_space: Map[String, Int],station_space: Map[String, Int]): Unit ={
//    this.th_window = th_window
//    this.dataType = dataType
//    this.dataInfo = dataInfo
//    this.gbno_space = gbno_space
//    this.station_space = station_space
//  }
  // ----------------------------------------------------------------------
//  private val th_window = ConfReader.getDFConf("DFusion/conf/DF.properties")
//  private var dataType = ConfReader.getDataConf("DFusion/conf/dataType.properties")
//  private var dataInfo = ConfReader.getDataConf("DFusion/conf/dataInfo.properties")
//  private val gbno_space = ConfReader.getSpaceConf("DFusion/conf/gbno_space.properties")
//  private val station_space = ConfReader.getSpaceConf("DFusion/conf/station_space.properties")
  // ---------------------------------------
//  val cl = this.getClass.getClassLoader
//  private val th_window = ConfReader.getDFConf(cl.getResource("DF.properties").getPath)
//  private var dataType = ConfReader.getDataConf(cl.getResource("dataType.properties").getPath)
//  private var dataInfo = ConfReader.getDataConf(cl.getResource("dataInfo.properties").getPath)
//  private val gbno_space = ConfReader.getSpaceConf(cl.getResource("gbno_space.properties").getPath)
//  private val station_space = ConfReader.getSpaceConf(cl.getResource("station_space.properties").getPath)
  // ---------------------------------------
  private val th_window = ConfReader.getDFConf("DF.properties")
  private var dataType = ConfReader.getDataConf("dataType.properties")
  private var dataInfo = ConfReader.getDataConf("dataInfo.properties")
  private val gbno_space = ConfReader.getSpaceConf("gbno_space.properties")
  private val station_space = ConfReader.getSpaceConf("station_space.properties")
  // ----------------------------------------------------------------------
  def getTh: Long = th_window("th")
  def getWindow: Long = th_window("window")
  def getThVsWindow: Double = th_window("th")*1.0/th_window("window")
  // ----------------------------------------------------------------------
  def getTypeKeys: List[String] = dataType.keySet.toList.sorted
  def getTypes(k: String): List[String] = dataType(k)
  def getBasePath(k: String): String = dataInfo(k).head
  def getTimeSpace(k: String): Array[String] = dataInfo(k)(1).split("-")
  def getCItems(k: String): Array[String] = dataInfo(k)(2).split("-")
  // ----------------------------------------------------------------------
  def checkDataInfo(): Unit = {
    for (k1 <- dataType.keySet.toList.sorted) {
      val newitems = new ListBuffer[String]
      for (k2 <- dataType(k1)) {
        breakable {
          println("# 【checkDataInfo】" + k1 + "->" + k2)
          val info = dataInfo(k2)
          if (info.size < 3) {
            println("# 【X1】" + info)
            dataInfo = dataInfo.-(k2)
            break
          }
          if (info(1).split("-").length != 2) {
            println("# 【X2】" + info)
            dataInfo = dataInfo.-(k2)
            break
          }
          if (info(2).split("-").length == 0) {
            println("# 【X3】" + info)
            dataInfo = dataInfo.-(k2)
            break
          }
          newitems.append(k2)
        }
        if (newitems.isEmpty) {
          dataType = dataType.-(k1)
        }
        else {
          dataType += (k1 -> newitems.toList)
        }
      }
    }
  }
  checkDataInfo()
  // ----------------------------------------------------------------------
  def getCn2: List[List[String]] = {
    val C = getTypeKeys
    val Cn2 = new ListBuffer[List[String]]
    for (i <- C.indices; j <- i + 1 until C.size) {
      Cn2.append(List(C(i), C(j)))
    }
    Cn2.toList
  }
  // ----------------------------------------------------------------------
  def getSpaceByGbno(k: String): Int = {
    var rs = -1
    try{
      rs = gbno_space(k)
    }catch {
      case e: Exception =>
    }
    rs
  }
  def getSpaceByStation(k: String): Int = {
    var rs = -1
    try{
      rs =  station_space(k)
    }catch {
      case e: Exception =>
    }
    rs
  }
  // ----------------------------------------------------------------------

  // ----------------------------------------------------------------------

  // ----------------------------------------------------------------------

  def main(args: Array[String]): Unit = {
//    println(CommonConfig.getTh)
//    println(CommonConfig.getWindow)
    // ---------------------------------------
//    println(CommonConfig.dataType)
//    println(CommonConfig.dataInfo)
    // ---------------------------------------
//    println(CommonConfig.getCn2)
//    val types = CommonConfig.getTypes("A")
//    println(types)
//    val dataTypeI = types.head
//    val basePath = CommonConfig.getBasePath(dataTypeI)
//    val ts = CommonConfig.getTimeSpace(dataTypeI)
//    val items = CommonConfig.getCItems(dataTypeI)
//    val tmp = ts ++ items
//    println(tmp.toList.toString())
//    println(basePath)
//    println(ts.toList.toString())
//    println(items(0))
    // ---------------------------------------
    println(getSpaceByGbno("44039605011400070000")) // 1263031000
    println(getSpaceByGbno("44039605011400070003")) // 1263031000
    println(getSpaceByStation("红树湾南")) // 1265015000

  }
}
