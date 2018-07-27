package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{
  ByteStringSerializer,
  CommandRequest,
  CommandResponse,
  StringParsersSupport
}
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }

final case class SetRequest[A](id: UUID, key: String, value: A)(implicit bs: ByteStringSerializer[A])
    extends CommandRequest
    with StringParsersSupport {

  override type Response = SetResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"""SET $key $value"""

  override def toByteString: ByteString = ByteString(s"SET $key ") ++ bs.serialize(value)

  override protected def responseParser: P[Expr] = simpleStringReply

  override def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (SetSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (SetSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait SetResponse                                                    extends CommandResponse
final case class SetSuspended(id: UUID, requestId: UUID)                    extends SetResponse
final case class SetSucceeded(id: UUID, requestId: UUID)                    extends SetResponse
final case class SetFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetResponse
