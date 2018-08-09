package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.keys.Status.{ NoKey, Ok }
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import enumeratum._
import fastparse.all._

import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

final case class MigrateRequest(id: UUID,
                                host: String,
                                port: Int,
                                key: String,
                                toDbNo: Int,
                                timeout: FiniteDuration,
                                copy: Boolean,
                                replace: Boolean,
                                keys: NonEmptyList[String])
    extends CommandRequest
    with StringParsersSupport {

  override type Response = MigrateResponse

  override val isMasterOnly: Boolean = true

  override def asString: String =
    s"""MIGRATE $host $port $key $toDbNo ${timeout.toMillis}""" + (if (copy) " COPY" else "") + (if (replace) " REPLACE"
                                                                                                 else "")

  override protected lazy val responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (MigrateSucceeded(UUID.randomUUID(), id, Ok), next)
    case (SimpleExpr(NOKEY), next) =>
      (MigrateSucceeded(UUID.randomUUID(), id, NoKey), next)
    case (SimpleExpr(QUEUED), next) =>
      (MigrateSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (MigrateFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait Status extends EnumEntry
object Status extends Enum[Status] {
  override def values: immutable.IndexedSeq[Status] = findValues
  case object Ok    extends Status
  case object NoKey extends Status
}

sealed trait MigrateResponse                                                    extends CommandResponse
final case class MigrateSucceeded(id: UUID, requestId: UUID, status: Status)    extends MigrateResponse
final case class MigrateSuspended(id: UUID, requestId: UUID)                    extends MigrateResponse
final case class MigrateFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends MigrateResponse
