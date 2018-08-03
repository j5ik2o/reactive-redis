package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final case class MSetNxRequest(id: UUID, values: Map[String, Any]) extends CommandRequest with StringParsersSupport {

  override type Response = MSetNxResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = {
    val keyWithValues = values.foldLeft("") {
      case (r, (k, v)) =>
        r + s""" $k "$v""""
    }
    s"MSETNX $keyWithValues"
  }

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (MSetNxSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (MSetNxSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (MSetNxFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait MSetNxResponse                                                    extends CommandResponse
final case class MSetNxSuspended(id: UUID, requestId: UUID)                    extends MSetNxResponse
final case class MSetNxSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends MSetNxResponse
final case class MSetNxFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends MSetNxResponse
