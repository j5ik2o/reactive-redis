package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

case class IncrByRequest(id: UUID, key: String, value: Int) extends CommandRequest with StringParsersSupport {

  override type Response = IncrByResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"INCRBY $key $value"

  override protected def responseParser: P[Expr] = P(integerReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (IncrBySucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (IncrBySuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (IncrByFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait IncrByResponse                                              extends CommandResponse
case class IncrBySuspended(id: UUID, requestId: UUID)                    extends IncrByResponse
case class IncrBySucceeded(id: UUID, requestId: UUID, value: Int)        extends IncrByResponse
case class IncrByFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends IncrByResponse
