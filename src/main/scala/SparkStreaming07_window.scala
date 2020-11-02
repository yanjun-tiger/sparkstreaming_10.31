import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zhouyanjun
 * @create 2020-11-02 9:14
 */
object SparkStreaming07_window {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(3))
    // 2 通过监控端口创建DStream，读进来的数据为一行行
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    val wordToOneDStream : DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_,1))
    val wordToOneByWindow: DStream[(String, Int)] = wordToOneDStream.window(Seconds(12),Seconds(6))

    val wordToCountDStream: DStream[(String, Int)] = wordToOneByWindow.reduceByKey(_+_)

    wordToCountDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
