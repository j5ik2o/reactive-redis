package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

abstract class AbstractRedisClientSpec(system: ActorSystem) extends AbstractActorSpec(system) {
  private var _redisClient: RedisClient                  = _
  private var _connectionPool: RedisConnectionPool[Task] = _

  def connectionPool: RedisConnectionPool[Task] = _connectionPool
  def redisClient: RedisClient                  = _redisClient

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val peerConfigs = Seq(PeerConfig(new InetSocketAddress("127.0.0.1", redisMasterServer.ports.get(0))))
    _connectionPool = createConnectionPool(peerConfigs)
    _redisClient = RedisClient()(system)
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
