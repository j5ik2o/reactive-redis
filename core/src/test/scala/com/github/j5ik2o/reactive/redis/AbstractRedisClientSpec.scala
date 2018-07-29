package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import cats.data.NonEmptyList
import monix.eval.Task

abstract class AbstractRedisClientSpec(system: ActorSystem) extends AbstractActorSpec(system) {

  private var _redisClient: RedisClient                  = _
  private var _connectionPool: RedisConnectionPool[Task] = _

  def connectionPool: RedisConnectionPool[Task] = _connectionPool
  def redisClient: RedisClient                  = _redisClient

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val peerConfigs = NonEmptyList.of(PeerConfig(new InetSocketAddress("127.0.0.1", redisMasterServer.getPort)))
    _connectionPool = createConnectionPool(peerConfigs)
    _redisClient = RedisClient()(system)
  }

  override protected def afterAll(): Unit = {
    _connectionPool.dispose()
    super.afterAll()
  }

  protected def runProgram[A](program: ReaderTTaskRedisConnection[A]): A = {
    connectionPool
      .withConnectionF { con =>
        program.run(con)
      }
      .runAsync
      .futureValue
  }

}
