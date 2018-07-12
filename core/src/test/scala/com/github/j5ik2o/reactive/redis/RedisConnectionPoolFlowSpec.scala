package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.time.ZonedDateTime
import java.util.UUID

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{ Flow, Sink, Source }
import com.github.j5ik2o.reactive.redis.command.{
  CommandRequest,
  CommandResponse,
  GetCommandRequest,
  SetCommandRequest
}

import scala.collection.immutable

class RedisConnectionPoolFlowSpec extends ActorSpec(ActorSystem("RedisConnectionPoolFlowSpec")) {

  val flow: Flow[CommandRequest, CommandResponse, NotUsed] = RedisConnectionPoolFlow(
    ConnectionPoolConfig(maxActive = 5),
    ConnectionConfig(new InetSocketAddress("127.0.0.1", 6379))
  )

  "RedisConnectionPoolFlow" - {
    "set & get" in {

      val results: immutable.Seq[CommandResponse] =
        Source(
          List(SetCommandRequest(UUID.randomUUID(), "a", ZonedDateTime.now().toString),
               GetCommandRequest(UUID.randomUUID(), "a"))
        ).via(flow)
          .runWith(Sink.seq)
          .futureValue

      println(results)

    }
  }
}
