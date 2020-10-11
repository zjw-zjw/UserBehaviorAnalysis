package com.zjw.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


// 输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

// 聚合结果样例类
case class PageViewCount(url: String, windowEnd: Long, count: Long)


object NetworkFlowTopNPage {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val inputStream = env.readTextFile("D:\\IdeaProjects\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apachetest.log")
    val inputStream = env.socketTextStream("localhost", 7777)

    // 转换成样例类类型， 指定timeStamp 和 waterMark
    val dataStream = inputStream
      .map( data => {
        val dataArray = data.split(" ")
        // 将时间字段 转换成 时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timeStamp: Long = simpleDateFormat.parse(dataArray(3)).getTime
        ApacheLogEvent(dataArray(0), dataArray(1), timeStamp, dataArray(5), dataArray(6))
      })
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
          override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
        })

    // 开窗聚合  三重保证
    val lateOutputTag = new OutputTag[ApacheLogEvent]("late data")
    val aggStream = dataStream
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
        .allowedLateness(Time.minutes(1))
        .sideOutputLateData( lateOutputTag )
      .aggregate(new PageCountAgg(), new PageCountWindowResult())

    // 定义侧输出流
    val lateDataStream = aggStream.getSideOutput(lateOutputTag)

    // 每个窗口的统计值，排序输出
    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotPage(3))

    dataStream.print("data")
    aggStream.print("agg")
    lateDataStream.print("late")
    resultStream.print("result")

    env.execute("top n page job")
  }
}


// 实现自定义的预聚合函数
class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 实现自定义窗口函数，包装成样例类输出
class PageCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getEnd, input.head))
  }
}


// 自定义Process Function
class TopNHotPage(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String]{

  // 定义ListState 保存所有聚合结果
  lazy val pageCountMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pagecount-map", classOf[String], classOf[Long]))


  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {
    pageCountMapState.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60 * 1000L)   // 用于清空状态
  }

  // 等待数据都到齐，从状态中 取出 排序 输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    if (timestamp == ctx.getCurrentKey + 60 * 1000L) {
      pageCountMapState.clear()
      return
    }

    val allPageCountList: ListBuffer[(String, Long)] =  ListBuffer()
    val iter = pageCountMapState.entries().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      allPageCountList += ((entry.getKey, entry.getValue))
    }


    val sortedPageCountList  = allPageCountList.sortWith(_._2 > _._2).take(n)  // 降序排列


    // 将排名信息格式化成字符串 String, 方便监控显示
    val result: StringBuilder = new StringBuilder()
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
    // 遍历sorted列表 输出top n 信息
    for (i <-  sortedPageCountList.indices) {
      val currentItemCount = sortedPageCountList(i)
      result.append("Top").append(i+1).append(":")
        .append(" 页面Url = ").append(currentItemCount._1)
        .append(" 访问量 = ").append(currentItemCount._2)
        .append("\n")
    }

    result.append("===========================\n\n")

    // 控制输出频率
    Thread.sleep(1000L)

    out.collect(result.toString())
  }
}