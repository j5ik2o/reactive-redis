package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class RedisClientSpec extends ActorSpec(ActorSystem("RedisClientSpec")) {
  var connectionPool: RedisConnectionPool[Task] = _
  var redisClient: RedisClient                  = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val connectionPoolConfig = ConnectionPoolConfig()
    val connectionConfig     = ConnectionConfig(new InetSocketAddress("127.0.0.1", redisServer.ports.get(0)))
    connectionPool = RedisConnectionPool[Task](connectionPoolConfig, connectionConfig)
    redisClient = RedisClient()
  }

  "RedisClient" - {
    "set & get" in {

      val program = for {
        _ <- redisClient.set("a", "1")
        v <- redisClient.get("a")
      } yield v

      val value = connectionPool
        .withConnection {
          program.run
        }
        .runAsync
        .futureValue

      println(value)

    }
  }

}
