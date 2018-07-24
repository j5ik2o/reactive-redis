package com.github.j5ik2o.reactive.redis.pool

import java.util.UUID

import cn.danielw.fop.Poolable
import com.github.j5ik2o.reactive.redis.command.CommandRequestBase
import com.github.j5ik2o.reactive.redis.{ PeerConfig, RedisConnection }
import monix.eval.Task

final case class FOPConnection(underlying: Poolable[RedisConnection]) extends RedisConnection {
  private val underlyingCon = underlying.getObject

  override def id: UUID = underlyingCon.id

  override def peerConfig: PeerConfig = underlyingCon.peerConfig

  override def shutdown(): Unit = underlyingCon.shutdown()

  override def send[C <: CommandRequestBase](cmd: C): Task[cmd.Response] = underlyingCon.send(cmd)

}
