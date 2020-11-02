package com.atguigu.util

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object JDBCUtil {

  //初始化连接池
  var dataSource: DataSource = init()

  //初始化连接池方法
  def init(): DataSource = {
    //传递必要的参数
    val properties = new Properties()
    val config: Properties = PropertiesUtil.load("config.properties")

    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", config.getProperty("jdbc.url"))
    properties.setProperty("username", config.getProperty("jdbc.user"))
    properties.setProperty("password", config.getProperty("jdbc.password"))
    properties.setProperty("maxActive", config.getProperty("jdbc.datasource.size"))
    //直接通过工厂来自动读取配置文件中的配置信息，并创建连接池对象
    DruidDataSourceFactory.createDataSource(properties)
  }

  //获取MySQL连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

  //执行SQL语句,单条数据插入        //第一个参数是连接，第二个参数是sql，后面是可变参数
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {

    var rtn = 0
    var pstmt: PreparedStatement = null

    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)

      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i))
        }
      }

      rtn = pstmt.executeUpdate() //executeUpdate用于执行 INSERT、UPDATE 或 DELETE 语句以及 SQL DDL（数据定义语言）语句

      connection.commit()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }

    rtn
  }

  //执行SQL语句,批量数据插入
  def executeBatchUpdate(connection: Connection, sql: String, paramsList: Iterable[Array[Any]]): Array[Int] = {

    var rtn: Array[Int] = null
    var pstmt: PreparedStatement = null

    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)

      for (params <- paramsList) {

        if (params != null && params.length > 0) {

          for (i <- params.indices) {
            pstmt.setObject(i + 1, params(i))
          }

          pstmt.addBatch()
        }
      }

      rtn = pstmt.executeBatch()

      connection.commit()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }

    rtn
  }

  //判断一条数据是否存在  连接，sql，参数。返回boolean值
  def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {

    var flag: Boolean = false
    var pstmt: PreparedStatement = null

    try {
      pstmt = connection.prepareStatement(sql)

      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }

      flag = pstmt.executeQuery().next()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }

    flag
  }

  //获取MySQL的一条数据
  def getDataFromMysql(connection: Connection, sql: String, params: Array[Any]): Long = {

    var result: Long = 0L
    var pstmt: PreparedStatement = null

    try {
      pstmt = connection.prepareStatement(sql)

      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }

      val resultSet: ResultSet = pstmt.executeQuery() //方法executeQuery,被用来执行 SELECT查询语句

      while (resultSet.next()) {
        result = resultSet.getLong(1)
      }

      resultSet.close()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }

    result
  }

  //主方法,用于测试上述方法
  def main(args: Array[String]): Unit = {

    //1 获取连接
    val connection: Connection = getConnection

    //2 预编译SQL
    val statement: PreparedStatement = connection.prepareStatement("select * from user_info where id > ?")

    //3 传输参数
    statement.setObject(1, 10)

    //4 执行sql
    val resultSet: ResultSet = statement.executeQuery()

    //5 获取数据
    while (resultSet.next()) {
      println("111:" + resultSet.getString(1))
    }

    //6 关闭资源
    resultSet.close()
    statement.close()
    connection.close()
  }
}
