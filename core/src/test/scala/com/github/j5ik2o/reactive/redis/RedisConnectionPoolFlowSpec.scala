package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.time.ZonedDateTime
import java.util.UUID

import akka.actor.ActorSystem
import akka.routing.DefaultResizer
import akka.stream.scaladsl.{ Sink, Source }
import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.command.CommandResponse
import com.github.j5ik2o.reactive.redis.command.strings.{ GetRequest, SetRequest }
import monix.eval.Task

import scala.collection.immutable
import cats.implicits._

class RedisConnectionPoolFlowSpec extends AbstractActorSpec(ActorSystem("RedisConnectionPoolFlowSpec")) {

  var pool: RedisConnectionPool[Task] = _

  override protected def createConnectionPool(peerConfigs: NonEmptyList[PeerConfig]): RedisConnectionPool[Task] = {
    val sizePerPeer = 2
    val lowerBound  = 1
    val upperBound  = 5
    val reSizer     = Some(DefaultResizer(lowerBound, upperBound))
    RedisConnectionPool.ofMultipleRoundRobin(
      sizePerPeer,
      peerConfigs,
      newConnection = RedisConnection.apply,
      reSizer = reSizer
    )
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val peerConfig = NonEmptyList.of(PeerConfig(new InetSocketAddress("127.0.0.1", redisMasterServer.getPort)))
    pool = createConnectionPool(peerConfig)
  }

  override protected def afterAll(): Unit = {
    pool.dispose()
    super.afterAll()
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
