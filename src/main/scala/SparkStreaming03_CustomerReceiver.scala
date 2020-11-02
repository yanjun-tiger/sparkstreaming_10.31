//import java.io.{BufferedReader, InputStreamReader}
//import java.net.Socket
//import java.nio.charset.StandardCharsets
//
//import org.apache.spark.SparkConf
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.receiver.Receiver
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
// * @author zhouyanjun
// * @create 2020-10-31 14:03
// */
//object SparkStreaming03_CustomerReceiver {
//  def main(args: Array[String]): Unit = {
//    // 2 初始化spark配置信息
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkStreaming")
//
//    //1 初始化sparkStreaming
//    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
//    //3 创建自定义receiver的streamng
//    ssc.receiverStream(new CustomerReceiver())
//
//  }
//
//  class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
//    //最初启动的时候，调用该方法，作用为：读数据并将数据发送给Spark
//    override def onStart(): Unit = {
//      new Thread("Socket Receiver") { //ctrl + o 调出要重写的方法
//        override def run() {
//          receive()
//        }
//      }.start()
//    }
//
//    //读数据并将数据发送给Spark
//    def receive(): Unit = {
//      //创建一个Socket
//      val socket: Socket = new Socket(host, port)
//      //创建一个BufferedReader用于读取端口传来的数据
//      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
//
//    }
//
//    //    读取数据
//
//
//    override def onStop(): Unit = ???
//  }
//
//}
