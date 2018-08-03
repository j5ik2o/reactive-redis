package com.github.j5ik2o.reactive.redis.experimental.jedis

import akka.stream.scaladsl.Flow
import com.github.j5ik2o.reactive.redis.command.{ CommandRequestBase, CommandResponse }
import redis.clients.jedis.Jedis

import scala.concurrent.duration.Duration
import scala.concurrent.{ ExecutionContext, Future }

object JedisFlow {

  def apply(host: String, port: Int, connectionTimeout: Option[Duration], socketTimeout: Option[Duration])(
      implicit ec: ExecutionContext
  ): Flow[CommandRequestBase, CommandResponse, Future[Jedis]] =
    Flow.fromGraph(new JedisFlowStage(host, port, connectionTimeout, socketTimeout))

}
