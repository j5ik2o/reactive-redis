package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import cats.Show
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

import scala.concurrent.duration.FiniteDuration

final case class PSetExRequest(id: UUID, key: String, millis: FiniteDuration, value: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = PSetExResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"""PSETEX $key ${millis.toMillis} "$value""""

  override protected def responseParser: P[Expr] = wrap(simpleStringReply)

  override protected def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (PSetExSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (PSetExSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (PSetExFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

object PSetExRequest {

  def apply[A](id: UUID, key: String, expires: FiniteDuration, value: A)(implicit s: Show[A]): PSetExRequest =
    new PSetExRequest(id, key, expires, s.show(value))

}

sealed trait PSetExResponse                                                    extends CommandResponse
final case class PSetExSuspended(id: UUID, requestId: UUID)                    extends PSetExResponse
final case class PSetExSucceeded(id: UUID, requestId: UUID)                    extends PSetExResponse
final case class PSetExFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends PSetExResponse
