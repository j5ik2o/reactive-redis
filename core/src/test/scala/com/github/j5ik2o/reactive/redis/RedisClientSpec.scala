package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.github.j5ik2o.reactive.redis.cmd.{ GetCommandRequest, GetSucceeded, SetCommandRequest, SetSucceeded }
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ FreeSpecLike, Matchers }

class RedisClientSpec extends TestKit(ActorSystem("RedisClient")) with FreeSpecLike with ScalaFutures with Matchers {
  "redisclient" - {
    "set" in {
      val c       = new RedisClient(new InetSocketAddress("127.0.0.1", 6379))
      val result1 = c.sendCommandRequest(SetCommandRequest(UUID.randomUUID(), "a", "1")).runAsync.futureValue
      result1.isInstanceOf[SetSucceeded] shouldBe true
      val result2 = c.sendCommandRequest(GetCommandRequest(UUID.randomUUID(), "a")).runAsync.futureValue
      result2.isInstanceOf[GetSucceeded] shouldBe true
    }
  }
}
