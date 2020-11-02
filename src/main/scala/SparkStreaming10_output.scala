import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author zhouyanjun
 * @create 2020-11-02 11:46
 */
object SparkStreaming10_output {
  def main(args: Array[String]): Unit = {
    // 1 初始化SparkStreamingContext
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkstreaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    // 保存数据到检查点
    ssc.checkpoint("./ck")

    // 2 通过监控端口创建DStream，读进来的数据为一行行
    val lines = ssc.socketTextStream("hadoop102", 9999)

    // 3 切割=》变换 。选择map or flatmap就是看想把数据一进一出，还是一进多出
    val wordToOne = lines.flatMap(_.split(" "))
      .map((_, 1))

    // 4 窗口
    val wordToSumDStream: DStream[(String, Int)] = wordToOne.reduceByKeyAndWindow(
      (x: Int, y: Int) => (x + y),
      (x: Int, y: Int) => (x - y),
      Seconds(12),//窗口
      Seconds(6),//滑动步长
      new HashPartitioner(2),
      (x: (String, Int)) => x._2 > 0
    )

    // 5 打印
    wordToSumDStream.foreachRDD(
      rdd=>{
        // 在Driver端执行(ctrl+n JobScheduler)，一个批次一次。因为rdd这里还没有执行，还只是在切分任务阶段
        // 在JobScheduler 中查找（ctrl + f）streaming-job-executor
        println("222222:" + Thread.currentThread().getName)

        rdd.foreachPartition(
          //5.1 测试代码
          iter=>iter.foreach(println)

          //5.2 企业代码
          //5.2.1 获取连接
          //5.2.2 操作数据，使用连接写库
          //5.2.3 关闭连接
        )
      }
    )

    // 6 启动=》阻塞
    ssc.start()
    ssc.awaitTermination()



  }

}
