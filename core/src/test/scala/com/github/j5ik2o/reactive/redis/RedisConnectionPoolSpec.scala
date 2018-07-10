package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.command.strings.{ GetRequest, SetRequest }
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class RedisConnectionPoolSpec extends AbstractActorSpec(ActorSystem("RedisClientPoolSpec")) {

  private var pool: RedisConnectionPool[Task] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val peerConfigs = Seq(
      PeerConfig(
        remoteAddress = new InetSocketAddress("127.0.0.1", redisMasterServer.ports().get(0))
      )
    )
    pool = createConnectionPool(peerConfigs)
  }

  override protected def afterAll(): Unit = {
    pool.dispose()
    super.afterAll()
  }
  "RedisConnectionPool" - {
    "set & get" in {
      val result = (for {
        _ <- ConnectionAutoClose(pool)(_.send(SetRequest(UUID.randomUUID(), "a", "a")))
        _ <- ConnectionAutoClose(pool) { _ =>
          Task.pure {
            println(s"pool.numActive = ${pool.numActive}")
            Thread.sleep(10 * 5)
          }
        }
        r <- ConnectionAutoClose(pool)(_.send(GetRequest(UUID.randomUUID(), "a")))
      } yield r).run().runAsync.futureValue
      println(result)
    }
  }

}
