package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.time.ZonedDateTime
import java.util.UUID

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.command.CommandResponse
import com.github.j5ik2o.reactive.redis.command.strings.{ GetRequest, SetRequest }
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future
import cats.data.ReaderT
class RedisConnectionPoolSpec extends ActorSpec(ActorSystem("RedisClientPoolSpec")) {

  var pool: RedisConnectionPool[Task] = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    pool = RedisConnectionPool[Task](
      ConnectionPoolConfig(),
      ConnectionConfig(new InetSocketAddress("127.0.0.1", redisServer.ports().get(0)))
    )
  }

  "RedisClientPool" - {
    "set & get" in {
      val futures: Seq[Future[CommandResponse]] = for (i <- 1 to 100)
        yield {
          pool
            .withConnection(
              ReaderT { con =>
                for {
                  _ <- con.send(SetRequest(UUID.randomUUID(), "a", ZonedDateTime.now().toString))
                  r <- con.send(GetRequest(UUID.randomUUID(), "a"))
                } yield r
              }
            )
            .runAsync
            .map { v =>
              println(v)
              v
            }
        }
      Future.sequence(futures).futureValue
      pool.dispose()
    }
  }

}
