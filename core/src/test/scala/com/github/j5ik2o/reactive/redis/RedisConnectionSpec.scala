package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.command.{ GetCommandRequest, GetSucceeded, SetCommandRequest, SetSucceeded }
import monix.execution.Scheduler.Implicits.global

class RedisConnectionSpec extends ActorSpec(ActorSystem("RedisClientSpec")) {

  val client = new RedisConnection(ConnectionConfig(new InetSocketAddress("127.0.0.1", 6379)))

  "redisclient" - {
    "set & get" in {
      val result1 = client.send(SetCommandRequest(UUID.randomUUID(), "a", "1")).runAsync.futureValue
      result1.isInstanceOf[SetSucceeded] shouldBe true

      val result2 = client.send(GetCommandRequest(UUID.randomUUID(), "a")).runAsync.futureValue
      result2.isInstanceOf[GetSucceeded] shouldBe true

    }
  }
}
