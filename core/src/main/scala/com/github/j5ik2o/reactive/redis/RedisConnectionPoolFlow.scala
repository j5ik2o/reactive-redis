package com.github.j5ik2o.reactive.redis

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse }
import monix.eval.Task

object RedisConnectionPoolFlow {

  def apply(connectionPoolConfig: ConnectionPoolConfig, connectionConfig: ConnectionConfig, parallelism: Int = 1)(
      implicit system: ActorSystem
  ): Flow[CommandRequest, CommandResponse, NotUsed] =
    new RedisConnectionPoolFlow(connectionPoolConfig, connectionConfig, parallelism).toFlow

}

private class RedisConnectionPoolFlow(connectionPoolConfig: ConnectionPoolConfig,
                                      connectionConfig: ConnectionConfig,
                                      parallelism: Int = 1)(implicit system: ActorSystem) {

  private val connectionPool = new RedisConnectionPool[Task](connectionPoolConfig, connectionConfig)

  private def toFlow: Flow[CommandRequest, CommandResponse, NotUsed] =
    Flow[CommandRequest].mapAsync(parallelism) { cmd =>
      import monix.execution.Scheduler.Implicits.global
      connectionPool.withConnection(_.send(cmd)).runAsync
    }

}
