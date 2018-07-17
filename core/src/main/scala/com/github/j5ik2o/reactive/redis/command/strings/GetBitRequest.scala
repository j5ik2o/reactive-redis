package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

case class GetBitRequest(id: UUID, key: String, offset: Int) extends CommandRequest with StringParsersSupport {

  override type Response = GetBitResponse

  override val isMasterOnly: Boolean = false

  override def asString: String = s"GETBIT $key $offset"

  override protected def responseParser: P[Expr] = P(integerReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (GetBitSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (GetBitSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (GetBitFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait GetBitResponse                                              extends CommandResponse
case class GetBitSuspended(id: UUID, requestId: UUID)                    extends GetBitResponse
case class GetBitSucceeded(id: UUID, requestId: UUID, value: Int)        extends GetBitResponse
case class GetBitFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends GetBitResponse
