package com.zjw.networkflow_analysis

object Test {
  def main(args: Array[String]): Unit = {

    val hashResult: Long = hash("abasdsa", 55)

    print("hashResult:" + hashResult)
  }



  // 实现一个hash函数
  def hash(str: String, seed: Int): Long = {
    val cap: Long = 1 << 30

    println(cap)

    var result = 0
    for ( i <- 0 until str.length ) {
      print("result * seed:"+ result * seed + "\t\t")
      result = result * seed + str.charAt(i)
      print("str.charAt(i):" + str.charAt(i) + "\t\t")
      print("result: result * seed + str.charAt(i):" + result + "\n")
    }

    print("result:" + result + "\n")
    // 返回一个在cap范围内的一个值
    // 举例： (0001000 - 1 ) & result = 0000111 & result
    (cap - 1) & result
  }
}
