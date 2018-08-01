package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }
import fastparse.all._

final case class IncrByFloatRequest(id: UUID, key: String, value: Double)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = IncrByFloatResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"INCRBYFLOAT $key $value"

  override protected def responseParser: P[Expr] = wrap(bulkStringReply | simpleStringReply)

  override protected def parseResponse: Handler = {
    case (StringOptExpr(s), next) =>
      (IncrByFloatSucceeded(UUID.randomUUID(), id, s.fold(0.0D)(_.toDouble)), next)
    case (SimpleExpr(QUEUED), next) =>
      (IncrByFloatSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (IncrByFloatFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait IncrByFloatResponse                                                    extends CommandResponse
final case class IncrByFloatSuspended(id: UUID, requestId: UUID)                    extends IncrByFloatResponse
final case class IncrByFloatSucceeded(id: UUID, requestId: UUID, value: Double)     extends IncrByFloatResponse
final case class IncrByFloatFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends IncrByFloatResponse
