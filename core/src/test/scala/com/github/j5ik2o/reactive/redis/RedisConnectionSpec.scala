package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.command.{ GetRequest, GetSucceeded, SetRequest, SetSucceeded }
import monix.execution.Scheduler.Implicits.global

class RedisConnectionSpec extends ActorSpec(ActorSystem("RedisClientSpec")) {

  var connection: RedisConnection = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    connection = new RedisConnection(ConnectionConfig(new InetSocketAddress("127.0.0.1", redisServer.ports.get(0))))
  }

  "redisclient" - {
    "set & get" in {
      val result1 = connection.send(SetRequest(UUID.randomUUID(), "a", "1")).runAsync.futureValue
      result1.isInstanceOf[SetSucceeded] shouldBe true

      val result2 = connection.send(GetRequest(UUID.randomUUID(), "a")).runAsync.futureValue
      result2.isInstanceOf[GetSucceeded] shouldBe true

    }
  }
}
