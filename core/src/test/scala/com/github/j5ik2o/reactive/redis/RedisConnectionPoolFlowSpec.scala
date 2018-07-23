package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.time.ZonedDateTime
import java.util.UUID

import akka.actor.ActorSystem
import akka.routing.DefaultResizer
import akka.stream.scaladsl.{ Sink, Source }
import com.github.j5ik2o.reactive.redis.command.CommandResponse
import com.github.j5ik2o.reactive.redis.command.strings.{ GetRequest, SetRequest }
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.collection.immutable

class RedisConnectionPoolFlowSpec extends AbstractActorSpec(ActorSystem("RedisConnectionPoolFlowSpec")) {

  var pool: RedisConnectionPool[Task] = _

  override protected def createConnectionPool(peerConfigs: Seq[PeerConfig]): RedisConnectionPool[Task] =
    RedisConnectionPool.ofRoundRobin(sizePerPeer = 10, peerConfigs, newConnection = {
      RedisConnection(_, _)
    }, resizer = Some(DefaultResizer(lowerBound = 5, upperBound = 15)))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val peerConfig = Seq(PeerConfig(new InetSocketAddress("127.0.0.1", redisMasterServer.getPort)))
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
