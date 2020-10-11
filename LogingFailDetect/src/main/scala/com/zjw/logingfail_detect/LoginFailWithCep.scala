package com.zjw.logingfail_detect

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    // 从文件中读取数据，map成样例类，并分配时间戳和watermark
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3L)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })

    // 1.定义一个匹配的模式
//    val loginFailPattern: Pattern



    env.execute("login fail with cep job")
  }
}
