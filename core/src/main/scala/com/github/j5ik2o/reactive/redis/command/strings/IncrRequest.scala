package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

case class IncrRequest(id: UUID, key: String) extends CommandRequest with StringParsersSupport {

  override type Response = IncrResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"INCR $key"

  override protected def responseParser: P[Expr] = P(integerReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (IncrSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (IncrSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (IncrFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait IncrResponse                                              extends CommandResponse
case class IncrSuspended(id: UUID, requestId: UUID)                    extends IncrResponse
case class IncrSucceeded(id: UUID, requestId: UUID, value: Int)        extends IncrResponse
case class IncrFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends IncrResponse
