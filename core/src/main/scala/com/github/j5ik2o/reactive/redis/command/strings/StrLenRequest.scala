package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class StrLenRequest(id: UUID, key: String) extends CommandRequest with StringParsersSupport {

  override type Response = StrLenResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"STRLEN $key"

  override protected def responseParser: P[Expr] = P(integerReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (StrLenSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (StrLenSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (StrLenFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait StrLenResponse                                                    extends CommandResponse
final case class StrLenSuspended(id: UUID, requestId: UUID)                    extends StrLenResponse
final case class StrLenSucceeded(id: UUID, requestId: UUID, length: Int)       extends StrLenResponse
final case class StrLenFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends StrLenResponse
