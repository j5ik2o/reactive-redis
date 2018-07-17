package com.github.j5ik2o.reactive.redis

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import com.github.j5ik2o.reactive.redis.command.{ CommandRequestBase, CommandResponse }
import monix.eval.Task
import monix.execution.Scheduler
import cats.data.ReaderT

object RedisConnectionPoolFlow {

  def apply(redisConnectionPool: RedisConnectionPool[Task], parallelism: Int = 1)(
      implicit system: ActorSystem,
      scheduler: Scheduler
  ): Flow[CommandRequestBase, CommandResponse, NotUsed] =
    new RedisConnectionPoolFlow(redisConnectionPool, parallelism).toFlow

}

class RedisConnectionPoolFlow(redisConnectionPool: RedisConnectionPool[Task], parallelism: Int)(
    implicit system: ActorSystem
) {

  private def toFlow(implicit scheduler: Scheduler): Flow[CommandRequestBase, CommandResponse, NotUsed] =
    Flow[CommandRequestBase].mapAsync(parallelism) { cmd =>
      redisConnectionPool.withConnectionM(ReaderT(_.send(cmd))).runAsync
    }

}
