package com.My.Utils

import java.io.{File, FileNotFoundException}
import java.util.Properties

import org.apache.log4j.Logger

import scala.io.Source

/**
  * Created by:
  * User: liuzhao
  * Date: 2018/5/17
  * Email: liuzhao@66law.cn
  */
object Conf {
  private val log = Logger.getLogger(this.getClass.getSimpleName)
  private val properties = new Properties()
  private val path: String = "conf/myCrawl.conf" //文件要放到resource文件夹下
  private val defaultPath: String = "myCrawl.conf"
  try{
    log.info(s"load properties from $path")
    properties.load(Source.fromFile(path).reader())
  }catch {
    case e: FileNotFoundException =>
      try {
        val path1 = new File(path).getAbsolutePath
        println(path1)
        log.info(s"$path does not exist, try to load properties from default file $defaultPath")
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
