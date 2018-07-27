package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import akka.routing.DefaultResizer
import cats.data.NonEmptyList
import cats.implicits._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class RedisMasterSlavesConnectionSpec extends AbstractActorSpec(ActorSystem("RedisClientSpec")) {

  var connection: RedisMasterSlavesConnection = _

  val redisClient = RedisClient()

  override protected def createConnectionPool(peerConfigs: NonEmptyList[PeerConfig]): RedisConnectionPool[Task] =
    RedisConnectionPool.ofMultipleRoundRobin(sizePerPeer = 10,
                                             peerConfigs,
                                             RedisConnection(_, _),
                                             reSizer = Some(DefaultResizer(lowerBound = 5, upperBound = 15)))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    startSlaveServers(1)

    val masterPeerConfig = PeerConfig(new InetSocketAddress("127.0.0.1", redisMasterServer.getPort))
    val slavePeerConfigs =
      redisSlaveServers
        .map(
          slaveServer => PeerConfig(new InetSocketAddress("127.0.0.1", slaveServer.getPort))
        )
        .toList

    connection = new RedisMasterSlavesConnection(
      masterConnectionPoolFactory = RedisConnectionPool.ofSingleRoundRobin(3, masterPeerConfig, RedisConnection(_, _)),
      slaveConnectionPoolFactory =
        RedisConnectionPool.ofMultipleRoundRobin(5,
                                                 NonEmptyList.of(slavePeerConfigs.head, slavePeerConfigs.tail: _*),
                                                 RedisConnection(_, _))
    )
    Thread.sleep((1000 * timeFactor).toInt)
  }

  override protected def afterAll(): Unit = {
    connection.shutdown()
    stopSlaveServers()
    super.afterAll()
  }

  "RedisMasterSlavesConnection" - {
    "set & get" ignore {
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val result = (for {
        _      <- redisClient.set(key, value)
        _      <- ReaderTTask.pure(waitFor())
        result <- redisClient.get(key)
      } yield result).run(connection).runAsync.futureValue
      result.value shouldBe Some(value)
    }
  }

}
