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

final class MigrateRequest(val id: UUID,
                           val host: String,
                           val port: Int,
                           val key: String,
                           val toDbNo: Int,
                           val timeout: FiniteDuration,
                           val copy: Boolean,
                           val replace: Boolean,
                           val keys: NonEmptyList[String])
    extends CommandRequest
    with StringParsersSupport {

  override type Response = MigrateResponse

  override val isMasterOnly: Boolean = true

  override def asString: String =
    cs(
      "MIGRATE",
      Some(host),
      Some(port.toString),
      Some(key),
      Some(toDbNo.toString),
      Some(timeout.toMillis.toString),
      if (copy) Some("COPY") else None,
      if (replace) Some("REPLACE") else None
    )

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

  override def equals(other: Any): Boolean = other match {
    case that: MigrateRequest =>
      id == that.id &&
      host == that.host &&
      port == that.port &&
      key == that.key &&
      toDbNo == that.toDbNo &&
      timeout == that.timeout &&
      copy == that.copy &&
      replace == that.replace &&
      keys == that.keys
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(id, host, port, key, toDbNo, timeout, copy, replace, keys)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"MigrateRequest($id, $host, $port, $key, $toDbNo, $timeout, $copy, $replace, $keys)"

}

object MigrateRequest {

  def apply(id: UUID,
            host: String,
            port: Int,
            key: String,
            toDbNo: Int,
            timeout: FiniteDuration,
            copy: Boolean,
            replace: Boolean,
            keys: NonEmptyList[String]): MigrateRequest =
    new MigrateRequest(id, host, port, key, toDbNo, timeout, copy, replace, keys)

  def unapply(
      self: MigrateRequest
  ): Option[(UUID, String, Int, String, Int, FiniteDuration, Boolean, Boolean, NonEmptyList[String])] =
    Some((self.id, self.host, self.port, self.key, self.toDbNo, self.timeout, self.copy, self.replace, self.keys))

  def create(id: UUID,
             host: String,
             port: Int,
             key: String,
             toDbNo: Int,
             timeout: FiniteDuration,
             copy: Boolean,
             replace: Boolean,
             keys: NonEmptyList[String]): MigrateRequest =
    apply(id, host, port, key, toDbNo, timeout, copy, replace, keys)
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
