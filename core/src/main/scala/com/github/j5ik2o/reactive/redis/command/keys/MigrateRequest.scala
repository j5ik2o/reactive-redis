package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandResponse, SimpleCommandRequest, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }

import scala.concurrent.duration.FiniteDuration

case class MigrateRequest(id: UUID, host: String, port: Int, key: String, toDbNo: Int, timeout: FiniteDuration)
    extends SimpleCommandRequest
    with StringParsersSupport {
  override type Response = MigrateResponse

  override def asString: String = s"""MIGRATE $host $port $key $toDbNo ${timeout.toMillis}"""

  override protected def responseParser: P[Expr] = StringParsers.simpleStringReply

  override protected def parseResponse: Handler = {
    case (SimpleExpr("OK"), next) =>
      (MigrateSucceeded(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (MigrateFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait MigrateResponse                                              extends CommandResponse
case class MigrateSucceeded(id: UUID, requestId: UUID)                    extends MigrateResponse
case class MigrateFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends MigrateResponse
