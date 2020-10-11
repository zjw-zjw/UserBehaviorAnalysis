package com.zjw.logingfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// 定义输入输出的样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, WarningMsg: String)


object LoginFail {
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

    // 用processFunction进行转换，如果遇到2秒内连续2次登陆失败，就输出报警
    val loginWarningStream: DataStream[Warning] = loginEventStream
      .keyBy(_.userId)
      .process(new LoginFailWarning(2))


    loginWarningStream.print()

    env.execute("login fail job")
  }
}


// 实现自定义的ProcessFunction
class LoginFailWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {

  // 定义List状态，用来保存2秒内所有的登陆失败事件
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("saved-loginfail", classOf[LoginEvent]))
  // 定义ValueState用来保存定时器的时间戳
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))


  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 判断当前数据是否是登陆失败
    if (value.eventType == "fail") {
      // 如果是失败，那么添加到listState里，如果没有注册过定时器，就注册定时器
      loginFailListState.add(value)
      if (timerTsState.value() == 0) {
        val ts = value.eventTime * 1000L + 2000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      }
    } else {
      // 如果是登陆成功，删除定时器，重新开始
      ctx.timerService().deleteEventTimeTimer(timerTsState.value())
      loginFailListState.clear()
      timerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    // 如果2秒后的定时器触发了，那么就要判断listState中失败的次数是否超过预定义的maxFailTimes
    val allLoginFailList: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]
    val iterator = loginFailListState.get().iterator()
    while (iterator.hasNext) {
      allLoginFailList += iterator.next()
    }

    if (allLoginFailList.length >= maxFailTimes) {
      out.collect( Warning(ctx.getCurrentKey,
        allLoginFailList.head.eventTime,
        allLoginFailList.last.eventTime,
        "login fail in 2s for " + allLoginFailList.length + " times.") )
    }

    // 清空状态
    loginFailListState.clear()
    timerTsState.clear()
  }
}