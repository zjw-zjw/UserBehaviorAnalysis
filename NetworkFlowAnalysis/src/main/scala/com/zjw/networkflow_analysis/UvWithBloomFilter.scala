package com.zjw.networkflow_analysis


import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UvWithBloomFilter {
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
      .map( data => ("uv", data.userId) )
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger( new MyTrigegr() )    // 自定义触发器
      .process( new UvCountResultWithBloomFilter() )



    uvStream.print()

    env.execute("uv job")
  }
}

// 自定义trigger 触发器， 每来一条数据就触发一次窗口计算操作
class MyTrigegr() extends Trigger[(String, Long), TimeWindow] {
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  // 数据来了之后，触发计算并清空状态，不保存数据
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE
}


// 自定义ProcessWindowFunction，把当前数据进行处理，位图保存在redis中
class UvCountResultWithBloomFilter() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{

  var jedis: Jedis = _
  var bloom: Bloom = _

  // 建立redis连接
  override def open(parameters: Configuration): Unit = {
    jedis = new Jedis("node01", 6379)
    // 位图大小： 2^30， 大约占用128MB
    bloom = new Bloom(1 << 30)
  }

  // 每来一个数据，主要是用布隆过滤器判断redis位图中对应位置是否为1
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // bitmap用当前窗口的key，保存到redis里，（windowEnd， bitmap）
    val storeKey = context.window.getEnd.toString

    // 把每个窗口的uv count值作为状态也存入redis中，存成一张叫做countMap的表
    // 窗口为key count值为value
    val countMap = "countMap"
    // 先获取当前的count值
    var count  = 0L
    if (jedis.hget(countMap, storeKey) != null) {
      count = jedis.hget(countMap, storeKey).toLong
    }

    // 取userId，计算hash值，判断是否在位图中
    val userID = elements.last._2.toString
    val offset: Long = bloom.hash(userID, 55)
    val isExist = jedis.getbit( storeKey, offset )

    // 如果不存在，那么就将对应位置置1， count+1； 如果存在，不做操作
    if (!isExist) {
      jedis.setbit(storeKey, offset, true)
      jedis.hset( countMap, storeKey, (count+1).toString)
    }
  }
}

// 自定义一个布隆过滤器  (概率性的)
class Bloom(size: Long) extends Serializable{

  // 定义位图的大小, 应该是2的整次幂
  private val cap = size

  // 实现一个hash函数
  def hash(str: String, seed: Int): Long = {
    var result = 0
    for ( i <- 0 until str.length ) {
      result = result * seed + str.charAt(i)
    }

    // 返回一个在cap范围内的一个值
    // 举例： (0001000 - 1 ) & result = 0000111 & result
    (cap - 1) & result
  }



}