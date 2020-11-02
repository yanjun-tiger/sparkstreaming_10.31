import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zhouyanjun
 * @create 2020-10-31 15:01
 */
object SparkStreaming04_DirectAuto {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")

    //2.创建StreamingContext对象，作为程序的入口，传入参数配置信息，以及采集周期等上下文信息
    val ssc = new StreamingContext(sparkConf, Seconds(3))


    //4.定义Kafka参数，在map集合中定义内容，由key-value组成，key代表当前参数名称
    val kafkaPara: Map[String, Object] = Map[String, Object](
      // 指定kafka集群位置
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      // 作为消费者来讲，消费者组id
      ConsumerConfig.GROUP_ID_CONFIG -> "atguiguGroup",

      //消费数据的时候，key-value的序列化方式，第一种方式，可以指定字符串。第二种，通过类的形式（如下）。 指定消息的key的反序列化，全类名，通过反射获取反序列化器
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",

      // 指定消息的value的序列化。第二种方式：使用classof方法获取全类名，通过反射获取反序列化器
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )



    //3.通过第三方的工具类KafkaUtils来完成创建DStream抽象数据集，把读取到的Kafka数据创建为DStream;
    //从kafka读数据，把读到的数据封装成ConsumerRecord，是key-value类型，不指定泛型类型的话，会变成ConsumerRecord[Nothing, Nothing]不识别
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      // def createDirectStream[K, V] 需要指定泛型，否则是Any类型

      ssc, // StreamingContext对象

      //位置策略。参数：PreferConsistent策略，将kafka的某一分区交给某一Executor节点去处理
      LocationStrategies.PreferConsistent,

      //消费策略。参数：Subscribe消费topic：1）set集合内放入topic，从这个topic里读数据；2）kafka参数，要求放到map里面去；3）offset
      ConsumerStrategies.Subscribe[String, String](Set("testTopic"), kafkaPara) // 消费策略：（订阅多个主题，配置参数）
      // def Subscribe[K, V] 需要指定泛型，否则是Any类型

    )

    // 5.从kafuka读到的数据集：DStream已经封装好了，接下来就是对DStream数据集进行操作。因为现在里面的元素是ConsumerRecord对象而不是元组了，所以是不可能用_.2来操作元组的方法来操作
    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())//获取对象的value内容 //kafkaDStream.map(_.value())也可以

    //6.计算WordCount的一些不走
    valueDStream.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()

    //7.开启任务，开始采集数据
    ssc.start()
    ssc.awaitTermination()


  }

}



