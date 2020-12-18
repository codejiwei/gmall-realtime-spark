package com.codejiwei.gmall.realtime.spark.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object MyRedisUtil {

  private var jedisPool: JedisPool = null

  def getJedisClient = {
    if(jedisPool == null) {
      val config = MyPropertiesUtil.load("config.properties")
      val host = config.getProperty("redis.host")
      val port = config.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(10) //最大连接数
      jedisPoolConfig.setMaxIdle(5) //最大空闲
      jedisPoolConfig.setMinIdle(5) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(5000)  //忙碌时等待时长
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接进行测试

      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
    }
    jedisPool.getResource
  }

//  def main(args: Array[String]): Unit = {
//    val client = getJedisClient
//    println(client.ping())
//    client.close()
//  }

}
