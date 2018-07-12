package com.github.j5ik2o.reactive.redis

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse }
import monix.eval.Task

object RedisConnectionPoolFlow {

  def apply(connectionPoolConfig: ConnectionPoolConfig, connectionConfig: ConnectionConfig, parallelism: Int = 1)(
      implicit system: ActorSystem
  ): Flow[CommandRequest, CommandResponse, NotUsed] = {
    val connectionPool = new RedisConnectionPool[Task](connectionPoolConfig, connectionConfig)
    new RedisConnectionPoolFlow(connectionPool).toFlow
  }

  def apply(redisConnectionPool: RedisConnectionPool[Task])(
      implicit system: ActorSystem
  ): Flow[CommandRequest, CommandResponse, NotUsed] =
    new RedisConnectionPoolFlow(redisConnectionPool).toFlow

}

class RedisConnectionPoolFlow(redisConnectionPool: RedisConnectionPool[Task])(implicit system: ActorSystem) {

  private def toFlow: Flow[CommandRequest, CommandResponse, NotUsed] =
    Flow[CommandRequest].mapAsync(1) { cmd =>
      import monix.execution.Scheduler.Implicits.global
      redisConnectionPool.withConnection(_.send(cmd)).runAsync
    }

}
