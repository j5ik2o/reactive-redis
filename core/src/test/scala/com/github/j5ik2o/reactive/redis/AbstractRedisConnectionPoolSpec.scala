package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.command.strings.{ GetRequest, SetRequest }
import monix.eval.Task
import cats.implicits._

abstract class AbstractRedisConnectionPoolSpec(systemName: String) extends AbstractActorSpec(ActorSystem(systemName)) {

  private var pool: RedisConnectionPool[Task] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val peerConfigs = NonEmptyList.of(
      PeerConfig(
        remoteAddress = new InetSocketAddress("127.0.0.1", redisMasterServer.getPort),
        connectionBackoffConfig = None ///Some(BackoffConfig(maxRestarts = 1))
      )
    )
    pool = createConnectionPool(peerConfigs)
  }

  override protected def afterAll(): Unit = {
    pool.dispose()
    super.afterAll()
  }

  s"RedisConnectionPool_${UUID.randomUUID()}" - {
    "set & get" in {
      val result = (for {
        _ <- ConnectionAutoClose(pool)(_.send(SetRequest(UUID.randomUUID(), "a", "a")))
        _ <- ConnectionAutoClose(pool) { con =>
          Task.pure {
            println("connection.peerConfig = " + con.peerConfig.remoteAddress)
            println(s"pool.numActive = ${pool.numActive}")
            waitFor()
          }
        }
        r <- ConnectionAutoClose(pool)(_.send(GetRequest(UUID.randomUUID(), "a")))
      } yield r).run().runToFuture.futureValue
      println(result)
    }
  }

}
