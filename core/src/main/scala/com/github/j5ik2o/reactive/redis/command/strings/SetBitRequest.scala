package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class SetBitRequest(id: UUID, key: String, offset: Long, value: Long)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = SetBitResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"SETBIT $key $offset $value"

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (SetBitSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (SetBitSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SetBitFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait SetBitResponse                                                    extends CommandResponse
final case class SetBitSuspended(id: UUID, requestId: UUID)                    extends SetBitResponse
final case class SetBitSucceeded(id: UUID, requestId: UUID, value: Long)       extends SetBitResponse
final case class SetBitFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetBitResponse
