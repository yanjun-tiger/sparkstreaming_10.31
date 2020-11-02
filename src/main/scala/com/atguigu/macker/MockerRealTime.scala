package com.atguigu.macker
import java.util.Properties

import com.atguigu.util.{PropertiesUtil, RanOpt, RandomOptions}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

//城市信息表： city_id :城市id  city_name：城市名称   area：城市所在大区
case class CityInfo(city_id: Long, city_name: String, area: String)

object MockerRealTime {

  /**
   * 模拟的数据。实时数据生成模块
   * 格式 ：timestamp area city userid adid
   * 某个时间点 某个地区 某个城市 某个用户 某个广告
   */
  def generateMockData(): Array[String] = {

    val array: ArrayBuffer[String] = ArrayBuffer[String]()

    val CityRandomOpt = RandomOptions(
      RanOpt(CityInfo(1, "北京", "华北"), 30), //value值出现的比例
      RanOpt(CityInfo(2, "上海", "华东"), 30),
      RanOpt(CityInfo(3, "广州", "华南"), 10),
      RanOpt(CityInfo(4, "深圳", "华南"), 20),
      RanOpt(CityInfo(5, "天津", "华北"), 10)
    )

    val random = new Random()

    // 模拟实时数据：
    // timestamp province city userid adid
    for (i <- 0 to 50) {  //50条数据

      val timestamp: Long = System.currentTimeMillis()//System.currentTimeMillis()产生一个当前的毫秒，这个毫秒其实就是自1970年1月1日0时起的毫秒数
      val cityInfo: CityInfo = CityRandomOpt.getRandomOpt
      val city: String = cityInfo.city_name
      val area: String = cityInfo.area
      val adid: Int = 1 + random.nextInt(6) //random.nextInt方法的作用是生成一个随机的int值，该值介于[0,n)的区间，也就是0到n之间的随机int值，包含0而不包含n。
      val userid: Int = 1 + random.nextInt(6)

      // 拼接实时数据
      array += timestamp + " " + area + " " + city + " " + userid + " " + adid
    }

    array.toArray
  }

  def main(args: Array[String]): Unit = {

    // 获取配置文件config.properties中的Kafka配置参数
    val config: Properties = PropertiesUtil.load("config.properties")
    val brokers: String = config.getProperty("kafka.broker.list")
    val topic: String = config.getProperty("kafka.topic")

    // 创建配置对象
    val prop = new Properties() //Properties（Java.util.Properties），该类主要用于读取Java的配置文件

    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers) //通过调用基类的put方法来设置 键值对。
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    // 根据配置创建Kafka生产者
    val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](prop)

    while (true) {

      // 随机产生实时数据并通过Kafka生产者发送到Kafka集群中
      for (line <- generateMockData()) {
        //KafkaProducer.send(ProducerRecord),发送消息
        kafkaProducer.send(new ProducerRecord[String, String](topic, line))//一个ProducerRecord表示一条待发送的消息记录，主要由5个字段构成;  //这里选的是topic-所属topic;和value-消息体

        println(line)
      }

      Thread.sleep(2000)
    }
  }
}
