package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.github.j5ik2o.reactive.redis.cmd.{ GetCommandRequest, SetCommandRequest }
import monix.execution.Scheduler.Implicits.global
import org.scalatest.FreeSpecLike
import org.scalatest.concurrent.ScalaFutures

class RedisClientSpec extends TestKit(ActorSystem("RedisClient")) with FreeSpecLike with ScalaFutures {
  "redisclient" - {
    "set" in {
      val c       = new RedisClient(new InetSocketAddress("127.0.0.1", 6379))
      val result1 = c.sendCommandRequest(SetCommandRequest(UUID.randomUUID(), "a", "1")).runAsync.futureValue
      println(result1)
      val result2 = c.sendCommandRequest(GetCommandRequest(UUID.randomUUID(), "a")).runAsync.futureValue
      println(result2)
    }
  }
}
