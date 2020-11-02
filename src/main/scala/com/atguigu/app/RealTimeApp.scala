package com.atguigu.app

import java.util.{Date, Properties}

import com.atguigu.handler.{BlackListHandler, DateAreaCityAdCountHandler, LastHourAdCountHandler}
import com.atguigu.util.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimeApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("RealTimeApp ").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //3.读取数据
    val properties: Properties = PropertiesUtil.load("config.properties")
    val topic: String = properties.getProperty("kafka.topic")

    //从kafka读取到的数据，抽象数据集。封装成ConsumerRecord类型数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)

    //4.获取到从kafka里的数据。<-------通过将从Kafka获取的数据进行切分，并将切分的内容封装为Ads_log对象。
    val adsLogDStream: DStream[Ads_log] = kafkaDStream.map(kafkaData => { //获取到的每一行kafka数据

      val value: String = kafkaData.value() //数据获取到了
      val arr: Array[String] = value.split(" ")//用空格切分，得到一个字符串数组

      //因为是样例类，自动调用apply()，自动生成对象。往对象里传入参数
      Ads_log(arr(0).toLong, arr(1), arr(2), arr(3), arr(4)) //scala语言，取数组元素用(数字)
    })

    //5.TODO 需求1.0：根据MySQL中的黑名单过滤---> 然后得到过滤后数据集
    val filterAdsLogDStream: DStream[Ads_log] = BlackListHandler.filterByBlackList(adsLogDStream)

    //6.TODO 需求1.1：在需求1.0的基础上，把满足要求的用户写入黑名单
    BlackListHandler.addBlackList(filterAdsLogDStream)

    //测试打印
    filterAdsLogDStream.cache()
    filterAdsLogDStream.count().print()

    //7.TODO 需求2.0：统计每天各大区各个城市广告点击总数并保存至MySQL中
    DateAreaCityAdCountHandler.saveDateAreaCityAdCountToMysql(filterAdsLogDStream)

    //8.TODO 需求3.0：统计最近一小时(2分钟)广告分时点击总数
    val adToHmCountListDStream: DStream[(String, List[(String, Long)])]
    = LastHourAdCountHandler.getAdHourMintToCount(filterAdsLogDStream)

    //9.打印
    adToHmCountListDStream.print()

    //启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}

//使用样例类封装输入数据。广告点击数据： 时间 地区 城市 用户id 广告id。把对象放到rdd中
case class Ads_log(timestamp: Long, area: String, city: String, userid: String, adid: String)


//老师建议，把输入数据和输出数据都封装为样例类


//通过对象的属性来访问我们想要的，要比通过下标访问数据清晰很多
//样例类：本身就是一个普通的类；
//但是加上case以后，会自动帮我们做一些事情，比如①会自动生成伴生对象②自动生成toString()、apply()、equals()等方法③帮我们自动解决序列化问题，好像是