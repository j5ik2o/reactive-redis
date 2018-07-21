package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import akka.routing.DefaultResizer
import cats.implicits._
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class RedisMasterSlavesConnectionSpec extends AbstractActorSpec(ActorSystem("RedisClientSpec")) {

  var connection: RedisMasterSlavesConnection = _

  val redisClient = RedisClient()

  override protected def createConnectionPool(peerConfigs: Seq[PeerConfig]): RedisConnectionPool[Task] =
    RedisConnectionPool.ofRoundRobin(sizePerPeer = 10, peerConfigs, newConnection = {
      RedisConnection(_)
    }, resizer = Some(DefaultResizer(lowerBound = 5, upperBound = 15)))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    startSlaveServers()

    val masterPeerConfig = PeerConfig(new InetSocketAddress("127.0.0.1", redisMasterServer.getPort))
    val slavePeerConfigs =
      redisSlaveServers.map(
        slaveServer => PeerConfig(new InetSocketAddress("127.0.0.1", slaveServer.getPort))
      )

    connection = new RedisMasterSlavesConnection(
      masterConnectionPoolFactory = RedisConnectionPool.ofRoundRobin(3, Seq(masterPeerConfig), RedisConnection(_)),
      slaveConnectionPoolFactory = RedisConnectionPool.ofRoundRobin(5, slavePeerConfigs, RedisConnection(_))
    )
  }

  override protected def afterAll(): Unit = {
    connection.shutdown()
    stopSlaveServers()
    super.afterAll()
  }

  "RedisMasterSlavesConnection" - {
    "set & get" in {
      val key   = UUID.randomUUID().toString
      val value = UUID.randomUUID().toString
      val result = (for {
        _      <- redisClient.set(key, value)
        _      <- ReaderTTask.pure(Thread.sleep((1000 * timeFactor).toInt))
        result <- redisClient.get(key)
      } yield result).run(connection).runAsync.futureValue
      result.value shouldBe Some(value)
    }
  }

}
