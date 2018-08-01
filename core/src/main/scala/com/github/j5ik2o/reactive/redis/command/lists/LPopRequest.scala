package com.github.j5ik2o.reactive.redis.command.lists

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }
import fastparse.all._

final case class LPopRequest(id: UUID, key: String) extends CommandRequest with StringParsersSupport {

  override type Response = LPopResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"LPOP $key"

  override protected def responseParser: P[Expr] = wrap(bulkStringReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (StringOptExpr(s), next) =>
      (LPopSucceeded(UUID.randomUUID(), id, s), next)
    case (SimpleExpr(QUEUED), next) =>
      (LPopSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (LPopFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait LPopResponse                                                        extends CommandResponse
final case class LPopSuspended(id: UUID, requestId: UUID)                        extends LPopResponse
final case class LPopSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends LPopResponse
final case class LPopFailed(id: UUID, requestId: UUID, ex: RedisIOException)     extends LPopResponse
