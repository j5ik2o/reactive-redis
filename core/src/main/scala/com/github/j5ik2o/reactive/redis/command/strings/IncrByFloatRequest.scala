package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }
import fastparse.all._

case class IncrByFloatRequest(id: UUID, key: String, value: Double) extends CommandRequest with StringParsersSupport {

  override type Response = IncrByFloatResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"INCRBYFLOAT $key $value"

  override protected def responseParser: P[Expr] = P(bulkStringReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (StringOptExpr(s), next) =>
      (IncrByFloatSucceeded(UUID.randomUUID(), id, s.get.toDouble), next)
    case (SimpleExpr(QUEUED), next) =>
      (IncrByFloatSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (IncrByFloatFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait IncrByFloatResponse                                              extends CommandResponse
case class IncrByFloatSuspended(id: UUID, requestId: UUID)                    extends IncrByFloatResponse
case class IncrByFloatSucceeded(id: UUID, requestId: UUID, value: Double)     extends IncrByFloatResponse
case class IncrByFloatFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends IncrByFloatResponse
