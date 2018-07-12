package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import monix.eval.Task

class RedisClientSpec extends ActorSpec(ActorSystem("RedisClientSpec")) {

  import monix.execution.Scheduler.Implicits.global

  val connectionPoolConfig: ConnectionPoolConfig = ConnectionPoolConfig()
  val connectionConfig: ConnectionConfig         = ConnectionConfig(new InetSocketAddress("127.0.0.1", 6379))

  val connectionPool: RedisConnectionPool[Task] = RedisConnectionPool[Task](connectionPoolConfig, connectionConfig)
  val redisClient                               = RedisClient()

  "RedisClient" - {
    "set & get" in {
      import connectionPool._

      val program = for {
        _ <- redisClient.set("a", "1")
        v <- redisClient.get("a")
      } yield v

      val value = withConnection {
        program.run
      }.runAsync.futureValue

      println(value)

    }
  }

}
