package com.util.tagutil

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * redis连接池
  */
object JedisConnectionPool {
  val config = new JedisPoolConfig()
  config.setMaxTotal(20)
  config.setMaxIdle(10)

  private val pool = new JedisPool(config,"mini1",6379,10000,"123456")
  def getConnection():Jedis={
    pool.getResource
  }

}
