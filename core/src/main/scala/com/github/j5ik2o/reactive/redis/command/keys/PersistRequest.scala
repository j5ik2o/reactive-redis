package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr }

case class PersistRequest(id: UUID, key: String) extends CommandRequest with StringParsersSupport {
  override type Response = PersistResponse

  override def asString: String = s"PERSIST $key"

  override protected def responseParser: P[Expr] = StringParsers.integerReply

  override protected def parseResponse: Handler = {
    case NumberExpr(n) =>
      PersistSucceeded(UUID.randomUUID(), id, n == 1)
    case ErrorExpr(msg) =>
      PersistFailed(UUID.randomUUID(), id, RedisIOException(Some(msg)))
  }

}

sealed trait PersistResponse                                               extends CommandResponse
case class PersistSucceeded(id: UUID, requestId: UUID, isRemoved: Boolean) extends PersistResponse
case class PersistFailed(id: UUID, requestId: UUID, ex: RedisIOException)  extends PersistResponse
