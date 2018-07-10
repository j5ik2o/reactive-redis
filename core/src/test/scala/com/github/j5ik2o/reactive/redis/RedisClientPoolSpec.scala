package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.cmd.SetCommandRequest

class RedisClientPoolSpec extends ActorSpec(ActorSystem("RedisClientPoolSpec")) {

  val pool = new RedisClientPool(PoolConfig(), ClientConfig(new InetSocketAddress("127.0.0.1", 6379)))

  "RedisClientPoool" - {
    "set & get" in {
      val clients = (for (i <- 1 to 100) yield { println(i); pool.borrowClient.get })
      clients.foreach(_.sendCommandRequest(SetCommandRequest(UUID.randomUUID(), "a", "1")))
      Thread.sleep(5000)
      pool.dispose()
    }
  }

}
