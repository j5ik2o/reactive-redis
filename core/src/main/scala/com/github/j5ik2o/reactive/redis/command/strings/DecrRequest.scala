package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class DecrRequest(id: UUID, key: String) extends CommandRequest with StringParsersSupport {

  override type Response = DecrResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("DECR", Some(key))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (DecrSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (DecrSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (DecrFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait DecrResponse                                                    extends CommandResponse
final case class DecrSuspended(id: UUID, requestId: UUID)                    extends DecrResponse
final case class DecrSucceeded(id: UUID, requestId: UUID, value: Long)       extends DecrResponse
final case class DecrFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends DecrResponse
