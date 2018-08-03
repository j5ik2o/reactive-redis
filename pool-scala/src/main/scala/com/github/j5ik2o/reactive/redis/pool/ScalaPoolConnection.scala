package com.github.j5ik2o.reactive.redis.pool

import java.util.UUID

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.github.j5ik2o.reactive.redis.command.CommandRequestBase
import com.github.j5ik2o.reactive.redis.{ PeerConfig, RedisConnection }
import io.github.andrebeat.pool.Lease
import monix.eval.Task
import monix.execution.Scheduler

private[redis] final case class ScalaPoolConnection(underlying: Lease[RedisConnection]) extends RedisConnection {

  private val underlyingCon = underlying.get()

  override def id: UUID = underlyingCon.id

  override def peerConfig: PeerConfig = underlyingCon.peerConfig

  override def shutdown(): Unit = underlyingCon.shutdown()

  override def toFlow[C <: CommandRequestBase](parallelism: Int)(
      implicit scheduler: Scheduler
  ): Flow[C, C#Response, NotUsed] = underlyingCon.toFlow(parallelism)

  override def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = underlyingCon.send(cmd)

}
