package com.github.j5ik2o.reactive.redis.cats.free

import java.util.UUID

import akka.actor.ActorSystem
import cats.implicits._
import com.github.j5ik2o.reactive.redis.{ ActorSpec, RedisFutureClient, RedisServerSupport }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class StringsFreeFeatureSpec
    extends ActorSpec(ActorSystem("StringsFreeFeatureSpec"))
    with RedisServerSupport
    with StringsFreeFeature {

  describe("StringsFreeFeature") {
    // --- APPEND
    it("should be able to APPEND") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key = UUID.randomUUID().toString
      val program = for {
        r1 <- append(key, "Hello")
        r2 <- append(key, ", ")
        r3 <- append(key, "World!")
        r4 <- get(key)
      } yield (r1, r2, r3, r4)
      val interpreter = new StringsInterpreter(redisFutureClient)
      val future      = program.foldMap(interpreter)
      val result      = future.futureValue
      assert(result._1.contains(5))
      assert(result._2.contains(7))
      assert(result._3.contains(13))
      assert(result._4.contains("Hello, World!"))
      redisFutureClient.dispose()
    }
    // --- BITCOUNT
//    it("should be able to BITCOUNT") {
//      val redisFutureClient =
//        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
//      val key = UUID.randomUUID().toString
//      val program = for {
//        _ <- set(key, "a")
//        n <- bitCount(key)
//      } yield n
//      val interpreter = new StringsInterpreter(redisFutureClient)
//      val future      = program.foldMap(interpreter)
//      val result      = future.futureValue
//      assert(result.contains(3))
//      redisFutureClient.dispose()
//    }
  }

}
