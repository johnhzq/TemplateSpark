class ApplyTest private { //添加private隐藏构造器
  var age: Int = _
  var name: String = _  //可以预留
  //重载的构造器和C#里面的public Teacher(){}类似
  def this(age: Int, name: String){
    this() //必须得调用一次主构造器
    this.age=age
    this.name=name
  }
  def sayHello(): Unit = {
    println("hello "+this.name)
  }
}

object ApplyTest {
  var instant: ApplyTest = _
  def apply(): ApplyTest = {
    if (instant == null){
      instant = new ApplyTest(10,"Tom")
    }
    instant
  }
  def getName(): Unit ={
    println(instant.name)
  }
}
