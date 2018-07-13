package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalacheck.{ Gen, Shrink }

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

  implicit val noShrink: Shrink[String] = Shrink.shrinkAny

  val gen = for {
    key   <- Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(_.nonEmpty)
    value <- Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(_.nonEmpty)
  } yield (key, value)

  "RedisClient" - {
    "set & get" in forAll(gen) {
      case (key, value) =>
        val program = for {
          _ <- redisClient.set(key, value)
          v <- redisClient.get(key)
        } yield v

        val result = connectionPool
          .withConnection {
            program.run
          }
          .runAsync
          .futureValue

        result should not be empty

    }
  }

}
