package com.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object JedisCon {

  val config: JedisPoolConfig = new JedisPoolConfig


  //设置最大连接数
  config.setMaxTotal(20)

  //最大空闲
  config.setMaxIdle(10)

  val pool =new JedisPool(config,"hadoop01",6879,10000)

}
