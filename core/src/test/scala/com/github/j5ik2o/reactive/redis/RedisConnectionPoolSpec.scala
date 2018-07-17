package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.command.strings.{ GetRequest, SetRequest }
import com.github.j5ik2o.reactive.redis.command.transactions.{ ExecRequest, ExecResponse, MultiRequest }
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

  getClass.getSimpleName.stripSuffix("Spec") - {
    "set & get" in {
      val tasks: Seq[Task[(Int, ExecResponse)]] = for (i <- 1 to 31)
        yield {
          pool
            .withConnectionF { con =>
              for {
                _ <- con.send(MultiRequest(UUID.randomUUID()))
                _ <- con.send(SetRequest(UUID.randomUUID(), "a", i.toString))
                _ <- Task.pure {
                  println(s"pool.numActive = ${pool.numActive}")
                  Thread.sleep(10 * 5)
                }
                _ <- con.send(GetRequest(UUID.randomUUID(), "a"))
                r <- con.send(ExecRequest(UUID.randomUUID()))
              } yield (i, r)
            }
        }

      Task
        .sequence(tasks)
        .map { e =>
          println(e); e
        }
        .runAsync
        .futureValue
    }
  }

}
