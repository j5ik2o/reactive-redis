package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.util.UUID

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.command.strings.{ GetRequest, GetSucceeded, SetRequest }
import com.github.j5ik2o.reactive.redis.command.transactions.{ ExecRequest, ExecResponse, MultiRequest }
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.duration._

class RedisConnectionPoolSpec extends ActorSpec(ActorSystem("RedisClientPoolSpec")) {

  var pool: RedisConnectionPool[Task] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    pool = RedisConnectionPool.ofCommons[Task](
      ConnectionPoolConfig(
        maxTotal = Some(30),
        maxIdle = Some(10),
        minIdle = Some(5),
        timeBetweenEvictionRuns = Some(3 seconds),
        // testOnCreate = Some(true),
        // testOnBorrow = Some(true),
        //testOnReturn = Some(true),
        testWhileIdle = Some(true),
        abandonedConfig = Some(
          AbandonedConfig(
            logAbandoned = Some(true),
            removeAbandonedOnBorrow = Some(true),
            removeAbandonedOnMaintenance = Some(true),
            removeAbandonedTimeout = Some(1 seconds),
            requireFullStackTrace = Some(true)
          )
        )
      ),
      ConnectionConfig(remoteAddress = new InetSocketAddress("127.0.0.1", redisServer.ports().get(0)))
    )
  }

  "RedisClientPool" - {
    "set & get" in {
      val tasks: Seq[Task[(Int, ExecResponse)]] = for (i <- 1 to 30)
        yield {
          pool
            .withConnectionF { con =>
              for {
                _ <- con.send(MultiRequest(UUID.randomUUID()))
                _ <- con.send(SetRequest(UUID.randomUUID(), "a", i.toString))
                _ <- Task.pure(Thread.sleep(10 * 5))
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
      pool.dispose()
    }
  }

}
