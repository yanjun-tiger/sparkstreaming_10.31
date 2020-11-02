package com.atguigu.handler

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date
import com.atguigu.app.Ads_log
import com.atguigu.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

//描述：实时统计每天各地区各城市各广告的点击总流量，并将其存入MySQL。
object DateAreaCityAdCountHandler {

  // 时间格式化对象
  private val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd") //DateFormat 类的子类——SimpleDateFormat

  // 根据黑名单过滤后的数据集，统计每天各大区各个城市广告点击总数并保存至MySQL中

  def saveDateAreaCityAdCountToMysql(filterAdsLogDStream: DStream[Ads_log]): Unit = {

    //1.统计每天各大区各个城市广告点击总数
    val dateAreaCityAdToCount: DStream[((String, String, String, String), Long)] = filterAdsLogDStream.map(
      ads_log => {

      //a.取出时间戳
      val timestamp: Long = ads_log.timestamp

      //b.格式化为日期字符串
      val dt: String = sdf.format(new Date(timestamp))// Date now = new Date();创建一个Date对象，获取当前时间

      //c.转换结构，聚合；返回
      ((dt, ads_log.area, ads_log.city, ads_log.adid), 1L)
    }).reduceByKey(_ + _)

    //2.将每个批次统计之后的数据集合写入MySQL数据，同时实现对原有数据的更新
    dateAreaCityAdToCount.foreachRDD(rdd => {

      //对每个分区单独处理
      rdd.foreachPartition(
        iter => { //迭代器
        //a.获取连接
        val connection: Connection = JDBCUtil.getConnection

        //b.写库
        iter.foreach { //对分区中的每个元素
          case ((dt, area, city, adid), ct) =>
          JDBCUtil.executeUpdate(
            connection,
            """
              |INSERT INTO area_city_ad_count (dt,area,city,adid,count)
              |VALUES(?,?,?,?,?)
              |ON DUPLICATE KEY
              |UPDATE count=count+?;
                        """.stripMargin, //如果key相同的话，要把count更新一下
            Array(dt, area, city, adid, ct, ct)
          )
        }

        //c.释放连接
        connection.close()
      })
    })
  }
}
