package com.zjw.networkflow_analysis

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

// 定义样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class PvCountView( windowEnd: Long, count: Long )



object PageView {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 文件读取数据
    val inputStream = env.readTextFile("D:\\IdeaProjects\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")


    // 将数据转换成样例类， 提取时间戳定义waterMark
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
    val pvStream: DataStream[PvCountView] = dataStream
      .filter(_.behavior == "pv")
//      .map( data => ("pv", 1L) )  // map成二元组
      .map( new MyMapper() )      // 自定义mapper, 将key均匀分配
      .keyBy(_._1)                // 把所有的数据分到一组做总计
      .timeWindow(Time.hours(1))  // 开一小时的滚动窗口
      .aggregate(new PvCountAgg(), new PvCountResult())

    // 把各分区的结果汇总起来
    val pvTotalStream: DataStream[PvCountView] = pvStream
      .keyBy(_.windowEnd)
      .process( new TotalPvCountResult() )
//      .sum("count")


    pvTotalStream.map(data => {
      val timeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      (timeFormatter.format(data.windowEnd), data.count)
    })
      .print()


    env.execute("pv job")
  }
}

// 自定义预聚合函数
class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long]{
  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数，把窗口信息包装成样例类并输出
class PvCountResult() extends WindowFunction[Long, PvCountView, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCountView]): Unit = {
    out.collect(PvCountView(window.getEnd, input.head))
  }
}


// 自定义MapperFunction，随机生成一个key
class MyMapper() extends RichMapFunction[UserBehavior, (String, Long)]{
  lazy val index: Int = getRuntimeContext.getIndexOfThisSubtask
  override def map(value: UserBehavior): (String, Long) = (index.toString, 1L)
}


// 自定义ProcessFunction,将聚合结果按窗口合并
class TotalPvCountResult() extends KeyedProcessFunction[Long, PvCountView, PvCountView]{
  // 定义一个状态，用来保存所有结果之和
  lazy val totalCountState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-count", classOf[Long]))

  override def processElement(value: PvCountView, ctx: KeyedProcessFunction[Long, PvCountView, PvCountView]#Context, out: Collector[PvCountView]): Unit = {
    val currentTotalCount = totalCountState.value()
    // 加上新的count值，更新状态
    totalCountState.update(currentTotalCount + value.count)
    // 注册定时器, windowEnd + 1之后触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCountView, PvCountView]#OnTimerContext, out: Collector[PvCountView]): Unit = {
    // 定时器触发时，所有分区的count值到达，输出总和
    out.collect(PvCountView(ctx.getCurrentKey, totalCountState.value()))
    totalCountState.clear()
  }
}