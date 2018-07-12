package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress
import java.time.ZonedDateTime
import java.util.UUID

import akka.actor.ActorSystem
import com.github.j5ik2o.reactive.redis.command.{ CommandResponse, GetCommandRequest, SetCommandRequest }
import monix.eval.Task

import scala.concurrent.Future

class RedisConnectionPoolSpec extends ActorSpec(ActorSystem("RedisClientPoolSpec")) {

  val pool = new RedisConnectionPool[Task](ConnectionPoolConfig(maxActive = 5),
                                           ConnectionConfig(new InetSocketAddress("127.0.0.1", 6379)))

  "RedisClientPool" - {
    "set & get" in {
      import monix.execution.Scheduler.Implicits.global
      val futures: Seq[Future[CommandResponse]] = for (i <- 1 to 100)
        yield {
          pool
            .withConnection { con =>
              for {
                _ <- con.send(SetCommandRequest(UUID.randomUUID(), "a", ZonedDateTime.now().toString))
                r <- con.send(GetCommandRequest(UUID.randomUUID(), "a"))
              } yield r
            }
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
