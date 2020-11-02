import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zhouyanjun
 * @create 2020-11-02 8:51
 */
object SparkStreaming04_DirectAuto_2 {


  def main(args: Array[String]): Unit = {
    //2
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    //1
    val ssc = new StreamingContext(conf, Seconds(3))

    //4
    val kafkaParams = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguiguGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->classOf[StringDeserializer]
    )

    //3
    val kafkaDStrream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("testTopic"), kafkaParams)
    )

    //5
    kafkaDStrream.map(_.value()).flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print()
    //6 开启任务，开始采集数据
    ssc.start()
    ssc.awaitTermination()
  }
}
