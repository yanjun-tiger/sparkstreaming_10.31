package com.atguigu.handler

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.app.Ads_log
import com.atguigu.util.JDBCUtil
import org.apache.spark.streaming.dstream.DStream

object BlackListHandler {
  //时间格式化对象
  private val sdf = new SimpleDateFormat("yyyy-MM-dd") //SimpleDateFormat 使得可以选择任何用户定义的日期/时间格式的模式。

  //TODO 首先判断点击用户是否在黑名单内，在里面，就不能要。不在里面，要保留
  //adsLogDStream:从Kafka读取的数据集。 这是第一步，所以这个方法应该放在前面。逻辑可能更通顺
  def filterByBlackList(adsLogDStream: DStream[Ads_log]): DStream[Ads_log] = {

    adsLogDStream.transform( //通过JDBC周期性（也就是一个批次一个批次）获取黑名单数据

      rdd => { //对每一批次的数据进行操作，进行返回

        rdd.filter( //我们过滤后的数据，保留在这个rdd数据集里
          adsLog => {
            // 首先获取连接,与mysql进行连接
            val connection: Connection = JDBCUtil.getConnection

            // 判断黑名单中是否存在该用户。读取数据库，首先应该查询
            val bool: Boolean = JDBCUtil.isExist(
              connection,
              """
                |select * from black_list where userid=?
                |""".stripMargin,
              Array(adsLog.userid)
            )

            // 关闭连接
            connection.close() //不关闭的话，可能出现资源耗尽，连接池连接不上的情况

            // 返回是否存在标记
            !bool //判断点击用户是否在黑名单中。在黑名单，true->false,舍弃。不在，false->true,用户id保留
          }
        )
      }
    )
  }



  //TODO 通过计算，往mysql的黑名单列表里添加符合要求的用户
  def addBlackList(filterAdsLogDSteam: DStream[Ads_log]): Unit = {

    //统计当前批次中单日每个用户点击每个广告的总次数

    //1.转换和累加：ads_log=>((date,user,adid),1) =>((date,user,adid),count)
    //得到计算后的数据集
    val dateUserAdToCount: DStream[((String, String, String), Long)] = filterAdsLogDSteam.map(
      adsLog => {

        //a.将毫秒时间戳转换为日期字符串。格式化
        val date: String = sdf.format(new Date(adsLog.timestamp))

        //b.返回值
        ((date, adsLog.userid, adsLog.adid), 1L) //这不就是（word，count）形式嘛。//1L，long类型，是为了避免数据太大可能报错的问题
      }
    ).reduceByKey(_ + _) //两两聚合，统计

    //2 写出
    //dateUserAdToCount是统计结果。
    //TODO 如果没有超过阈值，那么需要将当天的广告点击数量在mysql中进行更新
    dateUserAdToCount.foreachRDD( //对每批次的数据，我们来做操作
      rdd => {
        // foreachPartition以一个分区为单位进行数据的处理，可以一个分区创建一个连接对象，这样可以大幅度减少连接对象的数量，提升效率
        // 因为foreach方法会每一条数据创建连接，太低效

        //foreach方法是RDD算子，算子之外的代码是在Driver端执行，算子内的代码是在Executor端执行
        //这样就会涉及闭包操作，Driver端的数据就需要传递到Executor端，需要将数据进行序列化
        //数据库的连接对象是不能序列化的。因为客户端访问数据库服务器，是需要授权的，而把授权对象序列化成c，c是不能访问数据库的，如果能访问，授权就没有意义

        // 每个分区数据写出一次
        rdd.foreachPartition(
          iter => { //迭代器
            // 获取连接
            val connection: Connection = JDBCUtil.getConnection

            //用模式匹配更合适一些,对于迭代器里的每一个元素
            iter.foreach { //这里的foreach是一个集合中的方法而算子rdd.foreach涉及到数据的传递和序列化。注意这个地方的{}这个符号
              case ((dt, user, ad), count) =>
                // 向MySQL中user_ad_count表，更新累加点击次数;如果还没有数据，那么就新增
                JDBCUtil.executeUpdate( //第一个参数是连接，第二个参数是sql，后面Array(dt, user, ad, count, count)是来替换？的
                  connection,
                  """
                    |INSERT INTO user_ad_count (dt,userid,adid,count)
                    |VALUES (?,?,?,?)
                    |ON DUPLICATE KEY
                    |UPDATE count=count+?
                                """.stripMargin,
                  Array(dt, user, ad, count, count)
                )//sql语句中不能有空格，否则会报错！！！

              //TODO 如果发现统计数量超过点击阈值，那么将用户拉入到黑名单
                // 查询user_ad_count表，获取MySQL中广告点击总次数
                val ct: Long = JDBCUtil.getDataFromMysql(
                  connection, //连接对象
                  """
                    |select count from user_ad_count where dt=? and userid=? and adid =?
                    |""".stripMargin, //sql对象
                  Array(dt, user, ad) //数组对象
                )

                // 点击次数>30次，加入黑名单
                if (ct >= 30) { //往mysql里面进行操作。JDBC的更新数据的操作
                  JDBCUtil.executeUpdate(
                    connection,
                    """
                      |INSERT INTO black_list (userid)
                      |VALUES (?)
                      |ON DUPLICATE KEY
                      |update userid=?
                      |""".stripMargin,
                    Array(user, user)
                  )
                }
            }
            connection.close()
          }
        )
      }
    )
  }



}

