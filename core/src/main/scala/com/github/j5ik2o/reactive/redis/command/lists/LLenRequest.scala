package com.github.j5ik2o.reactive.redis.command.lists
import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class LLenRequest(id: UUID, key: String) extends CommandRequest with StringParsersSupport {

  override type Response = LLenResponse
  override val isMasterOnly: Boolean = false

  override def asString: String = s"LLEN $key"

  override protected def responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (LLenSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (LLenSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (LLenFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait LLenResponse                                                    extends CommandResponse
final case class LLenSucceeded(id: UUID, requestId: UUID, value: Long)       extends LLenResponse
final case class LLenSuspended(id: UUID, requestId: UUID)                    extends LLenResponse
final case class LLenFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends LLenResponse
