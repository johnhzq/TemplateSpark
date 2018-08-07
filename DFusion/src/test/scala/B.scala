import conf.Config

object B {
  def main(args: Array[String]): Unit = {
//    ApplyTest.getName()
    ApplyTest().sayHello()
    println(Config().getTh)
    println(Config().getSpaceByGbno("44039605011400070003"))
  }

}
