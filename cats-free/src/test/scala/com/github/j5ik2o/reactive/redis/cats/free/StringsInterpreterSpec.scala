package com.github.j5ik2o.reactive.redis.cats.free

import java.util.UUID

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.{ ActorSpec, RedisFutureClient, RedisServerSupport }
import org.scalatest.concurrent.ScalaFutures
import cats.implicits._
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global

class StringsInterpreterSpec
    extends ActorSpec(ActorSystem("RedisClientSpec"))
    with RedisServerSupport
    with ScalaFutures {

  describe("StringsInterpreter") {
    it("should be able to APPEND") {
      val redisFutureClient =
        RedisFutureClient(UUID.randomUUID, "127.0.0.1", testServer.getPort, 10 seconds)
      val key = UUID.randomUUID().toString
      val program = for {
        r1 <- StringsCommand.append(key, "A")
        r2 <- StringsCommand.append(key, "B")
        r3 <- StringsCommand.append(key, "C")
        r4 <- StringsCommand.get(key)
      } yield (r1, r2, r3, r4)
      val interpreter = new StringsInterpreter(redisFutureClient)
      val future      = program.foldMap(interpreter)
      val result      = future.futureValue
      assert(result._1.contains(1))
      assert(result._2.contains(2))
      assert(result._3.contains(3))
      assert(result._4.contains("ABC"))
    }
  }

}
