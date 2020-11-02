package com.atguigu.util

import java.io.InputStreamReader
import java.util.Properties

//读取资源文件工具类
object PropertiesUtil {

  def load(propertiesName: String): Properties = {

    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName), "UTF-8"))
    prop
  }
}
