package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

import scala.concurrent.duration.FiniteDuration

final case class MigrateRequest(id: UUID, host: String, port: Int, key: String, toDbNo: Int, timeout: FiniteDuration)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = MigrateResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = s"""MIGRATE $host $port $key $toDbNo ${timeout.toMillis}"""

  override protected def responseParser: P[Expr] = P(simpleStringReply)

  override protected def parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (MigrateSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (MigrateSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (MigrateFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait MigrateResponse                                                    extends CommandResponse
final case class MigrateSuspended(id: UUID, requestId: UUID)                    extends MigrateResponse
final case class MigrateSucceeded(id: UUID, requestId: UUID)                    extends MigrateResponse
final case class MigrateFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends MigrateResponse
