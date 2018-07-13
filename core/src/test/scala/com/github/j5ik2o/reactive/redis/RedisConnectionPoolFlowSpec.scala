package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.time.ZonedDateTime
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Sink, Source }
import com.github.j5ik2o.reactive.redis.command.{ CommandResponse, GetRequest, SetRequest }
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.collection.immutable

class RedisConnectionPoolFlowSpec extends ActorSpec(ActorSystem("RedisConnectionPoolFlowSpec")) {

  var pool: RedisConnectionPool[Task] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    pool = RedisConnectionPool[Task](
      ConnectionPoolConfig(),
      ConnectionConfig(new InetSocketAddress("127.0.0.1", redisServer.ports().get(0)))
    )
  }

  "RedisConnectionPoolFlow" - {
    "set & get" in {

      val results: immutable.Seq[CommandResponse] =
        Source(
          List(SetRequest(UUID.randomUUID(), "a", ZonedDateTime.now().toString), GetRequest(UUID.randomUUID(), "a"))
        ).via(RedisConnectionPoolFlow(pool))
          .runWith(Sink.seq)
          .futureValue

      println(results)

    }
  }
}
