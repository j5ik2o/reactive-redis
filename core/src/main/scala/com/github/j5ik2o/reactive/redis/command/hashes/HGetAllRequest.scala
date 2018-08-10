package com.github.j5ik2o.reactive.redis.command.hashes

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

final case class HGetAllRequest(id: UUID, key: String) extends CommandRequest with StringParsersSupport {

  override type Response = HGetAllResponse
  override val isMasterOnly: Boolean = false

  override def asString: String = cs("HGETALL", Some(key))

  override protected lazy val responseParser: P[Expr] = fastParse(stringArrayReply | simpleStringReply)

  override protected lazy val parseResponse: Handler = {
    case (ArrayExpr(values), next) =>
      (HGetAllSucceeded(UUID.randomUUID(), id, values.asInstanceOf[Seq[StringExpr]].map(_.value)), next)
    case (SimpleExpr(QUEUED), next) =>
      (HGetAllSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (HGetAllFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait HGetAllResponse                                                      extends CommandResponse
final case class HGetAllSuspended(id: UUID, requestId: UUID)                      extends HGetAllResponse
final case class HGetAllSucceeded(id: UUID, requestId: UUID, values: Seq[String]) extends HGetAllResponse
final case class HGetAllFailed(id: UUID, requestId: UUID, ex: RedisIOException)   extends HGetAllResponse
