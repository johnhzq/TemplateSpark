object Test {
  def main(args: Array[String]) {
    val pattern = "Scala".r
    val str = "Scala is Scalable and cool"
    val rs = pattern findFirstIn str
    println(rs.get)
    println(rs.isEmpty)

  }
}