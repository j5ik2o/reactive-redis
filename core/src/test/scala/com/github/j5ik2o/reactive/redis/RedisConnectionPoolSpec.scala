package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.command.SetCommandRequest
import cats.implicits._
class RedisConnectionPoolSpec extends ActorSpec(ActorSystem("RedisClientPoolSpec")) {

  val pool = new RedisConnectionPool(ConnectionPoolConfig(maxActive = 128),
                                     ConnectionConfig(new InetSocketAddress("127.0.0.1", 6379)))

  "RedisClientPoool" - {
    "set & get" in {
      val clients = (for (i <- 1 to 100) yield { println(i); pool.borrowClient.get })
      clients.foreach(_.send(SetCommandRequest(UUID.randomUUID(), "a", "1")))
      Thread.sleep(5000)
      pool.dispose()
    }
  }

}
