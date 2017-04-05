package com.github.j5ik2o.reactive.redis

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.actor.ActorSystem
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class RedisClientSpec
    extends ActorSpec(ActorSystem("RedisClientSpec"))
    with ServerBootable
    with ScalaFutures {

  val idGenerator = new AtomicLong()

  import system.dispatcher

  val redisClient =
    RedisClient(UUID.randomUUID, "127.0.0.1", testServer.address.get.getPort, Timeout(10 seconds))

  describe("RedisClient") {
    it("should be able to get a set value") {
      val key   = UUID.randomUUID().toString
      val value = "aaaa"

      val result = (for {
        _      <- redisClient.set(key, value)
        result <- redisClient.get(key)
      } yield result).futureValue
      assert(result.contains(value))
    }
  }

}