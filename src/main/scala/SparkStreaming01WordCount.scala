import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zhouyanjun
 * @create 2020-11-02 8:44
 */
object SparkStreaming01WordCount {
  def main(args: Array[String]): Unit = {
    //2
      val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksteaming")

    //1
    val ssc = new StreamingContext(conf, Seconds(3))
    //3
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    //4
    val wordToSumDStream : DStream[(String, Int)] = lineDStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //4
    wordToSumDStream.print()
    //5
    ssc.start()
    ssc.awaitTermination()

  }
}
