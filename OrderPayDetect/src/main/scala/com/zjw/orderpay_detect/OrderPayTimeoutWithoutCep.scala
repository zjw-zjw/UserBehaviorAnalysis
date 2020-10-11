package com.zjw.orderpay_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object OrderPayTimeoutWithoutCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据源，map成样例类，并分配watermark

    // 自定义ProcessFunction，实现精细化的流程控制

    // 打印输出， 主流输出正常的支付订单， 侧输出流输出超时数据
  }
}
