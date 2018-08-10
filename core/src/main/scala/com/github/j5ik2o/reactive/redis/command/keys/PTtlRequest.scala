package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class PTtlRequest(id: UUID, key: String) extends CommandRequest with StringParsersSupport {

  override type Response = PTtlResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = cs("PTTL", Some(key))

  override protected def responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (PTtlSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (PTtlSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (PTtlFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait PTtlResponse extends CommandResponse

final case class PTtlSucceeded(id: UUID, requestId: UUID, value: Long) extends PTtlResponse

final case class PTtlSuspended(id: UUID, requestId: UUID) extends PTtlResponse

final case class PTtlFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends PTtlResponse
