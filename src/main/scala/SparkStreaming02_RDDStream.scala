import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * 通过RDD队列方式创建DStream：数据来源于RDD队列，使用用于模拟流式数据，使用情况不多，用于测试
 * @author zhouyanjun
 * @create 2020-10-31 11:38
 */
object SparkStreaming02_RDDStream {



  def main(args: Array[String]): Unit = {
    //2 初始化spark配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    //1 初始化SparkStreamingContext
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(4))

    //3 创建RDD队列  创建空队列，从空队列中采集数据
    val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

    //4 创建QueueInputDStream   queueStream参数解读：1）队列；2）是否一次只取一个RDD
    val inputDStream: InputDStream[Int] = ssc.queueStream(rddQueue,oneAtATime = false)

    //5处理队列中的RDD数据  处理采集到的数据
    val sumDStream: DStream[Int] = inputDStream.reduce(_+_)
    //6 打印结果
    sumDStream.print()
    //7 启动任务 start底层是开了一个线程去执行采集数据，接下来采集完数据之后，对源源不断的数据进行处理
    ssc.start()

    //8 循环创建并向RDD队列中放入RDD
    for(i <- 1 to 5){
      rddQueue += ssc.sparkContext.makeRDD(1 to 2)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()
  }
}
