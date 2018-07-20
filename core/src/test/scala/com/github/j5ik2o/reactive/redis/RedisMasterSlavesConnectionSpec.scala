package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import cats.implicits._
import monix.execution.Scheduler.Implicits.global

class RedisMasterSlavesConnectionSpec extends AbstractActorSpec(ActorSystem("RedisClientSpec")) {

  var connection: RedisMasterSlavesConnection = _

  val redisClient = RedisClient()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    startSlaveServers()
    val masterPeerConfig = PeerConfig(new InetSocketAddress("127.0.0.1", redisMasterServer.ports.get(0)))
    val slavePeerConfigs =
      redisSlaveServers.map(
        slaveServer => PeerConfig(new InetSocketAddress("127.0.0.1", slaveServer.ports().get(0)))
      )
    connection = new RedisMasterSlavesConnection(
      masterConnectionFactory = RedisConnection(masterPeerConfig),
      slaveConnectionPoolFactory = RedisConnectionPool.ofRoundRobin(5, slavePeerConfigs, RedisConnection(_))
    )
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
        _ <- redisClient.set(key, value).map { v =>
          Thread.sleep(1 * 1000)
          v
        }
        result <- redisClient.get(key)
      } yield result).run(connection).runAsync.futureValue
      result.value shouldBe Some(value)
    }
  }

}
