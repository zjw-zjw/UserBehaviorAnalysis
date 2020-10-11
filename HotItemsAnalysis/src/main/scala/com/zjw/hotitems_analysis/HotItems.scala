package com.zjw.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer



// 定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 窗口聚合样例类
case class ItemViewCount( itemId: Long, windowEnd: Long, count: Long )


object HotItems {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 文件读取数据
//    val inputStream = env.readTextFile("D:\\IdeaProjects\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    // 从Kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "node01:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")   // 最新位置
    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))

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

    // 对数据进行转换，过滤出 pv 行为，开窗聚合统计个数
    val aggStream = dataStream
      .filter(_.behavior == "pv")
      .keyBy("itemId")      // 根据 商品ID 分组
      .timeWindow(Time.hours(1L), Time.minutes(5L))   // 定义滑动窗口
      .aggregate(new AggCount(), new ItemCountWindowResult())


    // 对窗口聚合结果，按窗口进行分组，并做排序取Top N输出
    val resultStream: DataStream[String] = aggStream
      .keyBy("windowEnd")
      .process( new TopNHotItems(5) )

    resultStream.print("result")

    env.execute("hot items job")
  }
}


// 自定义预聚合函数
class AggCount() extends AggregateFunction[UserBehavior, Long, Long]{
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L     // 赋初值

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}


// 扩展：自定义求平均值的聚合函数, 状态为 (sum, count)
class AvgAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {
  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) = {
    (accumulator._1 + value.timestamp, accumulator._2 + 1)
  }

  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def getResult(accumulator: (Long, Int)): Double = accumulator._1 * 1.0 / accumulator._2

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) =
    (a._1+b._1, a._2+b._2)
}




// 自定义窗口函数， 结合window信息，包装成样例类
class ItemCountWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}


// 自定义 KeyedProcessFunction
class TopNHotItems(n: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  // 定义一个list State 用来保存当前窗口所有保存的count结果
  lazy val itemCountListState: ListState[ItemViewCount] = getRuntimeContext
      .getListState(new ListStateDescriptor[ItemViewCount]("itemcount-list",  classOf[ItemViewCount]))
//  private var itemCountListState: ListState[ItemViewCount] = _
//  override def open(parameters: Configuration): Unit = {
//    super.open(parameters)
//    itemCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemcount-list",  classOf[ItemViewCount]))
//  }


  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每来一条数据，就把它保存到状态中
    itemCountListState.add(value)
    // 注册定时器，在windowEnd + 100ms 之后触发
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100L)
  }


  // 定时器触发时，从状态中取数据，排序输出
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 先把状态中的数据提取到一个ListBuffer中
    val allItemCountList: ListBuffer[ItemViewCount] = ListBuffer()

    import scala.collection.JavaConversions._
    for (itemCount <- itemCountListState.get()) {
      allItemCountList +=  itemCount
    }

    // 按照count值大小排序, 取Top N
    val sortedItemCountList = allItemCountList.sortBy(_.count)(Ordering.Long.reverse).take(n)

    // 清除状态
    allItemCountList.clear()

    // 将排名信息格式化成字符串 String, 方便监控显示
    val result: StringBuilder = new StringBuilder()
    result.append("时间: ").append(new Timestamp(timestamp - 100L)).append("\n")
    // 遍历sorted列表 输出top n 信息
    for (i <-  sortedItemCountList.indices) {
      val currentItemCount: ItemViewCount = sortedItemCountList(i)
      result.append("Top").append(i+1).append(":")
        .append(" 商品ID = ").append(currentItemCount.itemId)
        .append(" 访问量 = ").append(currentItemCount.count)
        .append("\n")
    }

    result.append("===========================\n\n")

    // 控制输出频率
    Thread.sleep(1000L)

    out.collect(result.toString())
  }
}
