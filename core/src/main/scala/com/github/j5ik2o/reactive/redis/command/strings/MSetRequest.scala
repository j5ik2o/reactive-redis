package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

case class MSetRequest(id: UUID, values: Map[String, Any]) extends CommandRequest with StringParsersSupport {

  override type Response = MSetResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = {
    val keyWithValues = values.foldLeft("") {
      case (r, (k, v)) =>
        r + s""" $k "$v""""
    }
    s"MSET $keyWithValues"
  }

  override protected def responseParser: P[Expr] = P(simpleStringReply)

  override protected def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (MSetSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (MSetSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (MSetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait MSetResponse                                              extends CommandResponse
case class MSetSuspended(id: UUID, requestId: UUID)                    extends MSetResponse
case class MSetSucceeded(id: UUID, requestId: UUID)                    extends MSetResponse
case class MSetFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends MSetResponse
