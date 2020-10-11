package com.zjw.networkflow_analysis

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class UvCount( windowEnd: Long, count: Long )


object UniqueVisitor {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从文件读取数据
    val inputStream = env.readTextFile("D:\\IdeaProjects\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")

    // 将数据转换成样例类类型，并且提取timeStamp定义watermark
    val dataStream: DataStream[UserBehavior] = inputStream.map(data => {
      val dataArray = data.split(",")
      val userId = dataArray(0).toLong
      val itemId = dataArray(1).toLong
      val categoryId = dataArray(2).toInt
      val behavior = dataArray(3)
      val timestamp = dataArray(4).toLong
      UserBehavior(userId, itemId, categoryId, behavior, timestamp)
    })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 分配key，包装成二元组聚合
    val uvStream: DataStream[UvCount] = dataStream
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))  // 基于dataStream开一小时的滚动窗口进行统计
//      .apply( new UvCountResult() )
      .aggregate( new UvCountAgg(), new UvCountResultWithIncreAgg())

    uvStream.map(data => {
      val timeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      (timeFormatter.format(data.windowEnd), data.count)
    })
      .print()

    env.execute("uv job")
  }
}

// 自定义全窗口函数
class UvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 定义一个set类型，保存所有的额userID
    var uvSet = Set[Long]()
    // 将当前窗口的所有数据，添加到set里面
    for ( userBehavior <- input ){
        uvSet += userBehavior.userId
    }

    // 输出set的大小，就是去重之后的uv值
    out.collect(UvCount(windowEnd = window.getEnd, count = uvSet.size))

  }
}

// 自定义预聚合函数
class UvCountAgg() extends AggregateFunction[UserBehavior, Set[Long], Long] {
  override def add(value: UserBehavior, accumulator: Set[Long]): Set[Long] = accumulator + value.userId

  override def createAccumulator(): Set[Long] = Set[Long]()

  override def getResult(accumulator: Set[Long]): Long = accumulator.size

  override def merge(a: Set[Long], b: Set[Long]): Set[Long] = a ++ b
}

// 自定义窗口函数，添加window信息，包装成样例类。将预聚合的结果输出
class UvCountResultWithIncreAgg() extends AllWindowFunction[Long, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[UvCount]): Unit = {
    out.collect( UvCount(window.getEnd, input.head) )
  }
}

