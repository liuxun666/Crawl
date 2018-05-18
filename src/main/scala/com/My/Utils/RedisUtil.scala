package com.My.Utils

import com.redis.RedisClientPool

/**
  * Created by:
  * User: liuzhao
  * Date: 2018/5/18
  * Email: liuzhao@66law.cn
  */
object RedisUtil {
  lazy val redisClientPool = new RedisClientPool(Conf.get("redis_host"), Conf.get("redis_port").toInt, Conf.get("redis_client_no").toInt, secret = Some(Conf.get("redis_auth")), database = Conf.get("redis_database").toInt)

  def rpop(key: Any): Option[String] = {
    redisClientPool.withClient(client => client.rpop(key))
  }

  def rpop(key: Any,num: Int): Seq[String] = {
    (0 to num).map(f => rpop(key)).filter(_.isDefined).map(_.get)
  }

  def lpush(key: Any,value: Any *): Option[Long] = {
    redisClientPool.withClient(client => client.lpush(key, value.head, value.drop(1)))
  }

  def llen(key: Any): Option[Long] = {
    redisClientPool.withClient(client => client.llen(key))
  }

  def exists(key: Any): Boolean = {
    redisClientPool.withClient(client => client.exists(key))
  }

}
