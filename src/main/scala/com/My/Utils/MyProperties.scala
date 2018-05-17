package com.My.Utils

import java.io.FileNotFoundException
import java.util.Properties

import org.slf4j.LoggerFactory

import scala.io.Source

/**
  * Created by:
  * User: liuzhao
  * Date: 2018/5/17
  * Email: liuzhao@66law.cn
  */
object MyProperties {
  private val log = LoggerFactory.getLogger(this.getClass.getSimpleName)
  lazy private val properties = new Properties()
  lazy val path: String = "conf/myCrawl.conf" //文件要放到resource文件夹下
  lazy val defaultPath: String = "myCrawl.conf"
  try{
    log.info(s"load properties from $path")
    properties.load(Source.fromFile(path).reader())
  }catch {
    case e: FileNotFoundException =>
      try {
        log.info(s"error when load properties from $path try to load properties from default file $defaultPath")
        properties.load(Source.fromInputStream(this.getClass.getResourceAsStream(defaultPath)).reader())
      } catch {
        case e: FileNotFoundException =>
          log.error("配置文件不存在", e)
        case e: Exception =>
          log.error("读取配置文件出错", e)
      }
  }

  def get(key: String): String = {
    properties.getProperty(key)
  }
  def get(key: String,defaultValue: String): String = {
    properties.getProperty(key,defaultValue)
  }

  def set(key: String,value: String): Unit ={
    properties.setProperty(key,value)
  }
}
