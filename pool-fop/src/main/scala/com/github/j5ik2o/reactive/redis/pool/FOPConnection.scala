package com.github.j5ik2o.reactive.redis.pool

import java.util.UUID

import akka.NotUsed
import akka.stream.scaladsl.Flow
import cn.danielw.fop.Poolable
import com.github.j5ik2o.reactive.redis.RedisConnection
import com.github.j5ik2o.reactive.redis.command.CommandRequestBase
import monix.eval.Task
import monix.execution.Scheduler

case class FOPConnection(underlying: Poolable[RedisConnection]) extends RedisConnection {
  private val underlyingCon = underlying.getObject

  override def id: UUID = underlyingCon.id

  override def shutdown(): Unit = underlyingCon.shutdown()

  override def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = underlyingCon.send(cmd)
}
