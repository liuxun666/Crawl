package com.My.Crawl

import com.My.Utils.{Conf, RedisUtil}

import scala.io.Source

/**
  * Created by:
  * User: liuzhao
  * Date: 2018/5/18
  * Email: liuzhao@66law.cn
  */
object ImportTask2Redis {
  def main(args: Array[String]): Unit = {
    println(Conf.get("redis_host"))
    println(Conf.get("redis_port").toInt)
    Conf.get("redis_client_no").toInt
    Some(Conf.get("redis_auth"))
    Conf.get("redis_database").toInt
    val ids = Source.fromFile("H:\\MyProject\\Crawl\\classes.csv").getLines().map(line => {
      val fields = line.split(",")
      fields(0) + "\001" + fields(1) + "\001" + 1
    }).toList
    RedisUtil.lpush("pages", ids)
  }
}
