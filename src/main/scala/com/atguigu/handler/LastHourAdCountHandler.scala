package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util.Date
import com.atguigu.app.Ads_log
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

//不同广告id，在不同时间段内（最近两分钟内）的点击总数
object LastHourAdCountHandler {

  //时间格式化对象，时间戳
  private val sdf: SimpleDateFormat = new SimpleDateFormat("HH:mm")

  // 过滤后的数据集，统计最近一小时(2分钟)广告分时点击总数
  def getAdHourMintToCount(filterAdsLogDStream: DStream[Ads_log]): DStream[(String, List[(String, Long)])] = {

    //1.开窗 => 时间间隔为1个小时 window() 2分钟。得到根据窗口计算出的数据集
    val windowAdsLogDStream: DStream[Ads_log] = filterAdsLogDStream.window(Minutes(2)) //滑动步长，这种形式，默认批次大小

    //2.转换数据结构！！！  ads_log =>((adid,hm),1L) map()
    val adHmToOneDStream: DStream[((String, String), Long)] = windowAdsLogDStream.map(
      adsLog => {

      val timestamp: Long = adsLog.timestamp
      val hm: String = sdf.format(new Date(timestamp)) //转换为需求需要的相应的时间

      ((adsLog.adid, hm), 1L) //只取我需要的
    })

    //3.统计总数 ((adid,hm),1L)=>((adid,hm),sum) reduceBykey(_+_)
    val adHmToCountDStream: DStream[((String, String), Long)] = adHmToOneDStream.reduceByKey(_ + _)

    //4.转换数据结构 ((adid,hm),sum)=>(adid,(hm,sum)) map() 广告id，时间段，点击计数
    val adToHmCountDStream: DStream[(String, (String, Long))] = adHmToCountDStream.map {
      case ((adid, hm), count) =>
      (adid, (hm, count))
    }

    //5.按照adid分组 (adid,(hm,sum))=>(adid,Iter[(hm,sum),...]) groupByKey。就是为了排序
    adToHmCountDStream
      .groupByKey()
      .mapValues(iter => iter.toList.sortWith(_._1 < _._1))
  }
}
