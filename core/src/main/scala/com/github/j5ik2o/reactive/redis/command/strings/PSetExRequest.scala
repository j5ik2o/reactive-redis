package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import cats.Show
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

import scala.concurrent.duration.FiniteDuration

final class PSetExRequest(val id: UUID, val key: String, val millis: FiniteDuration, val value: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = PSetExResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("PSETEX", Some(key), Some(millis.toMillis.toString), Some(value))

  override protected lazy val responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (PSetExSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (PSetExSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (PSetExFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: PSetExRequest =>
      id == that.id &&
      key == that.key &&
      millis == that.millis &&
      value == that.value
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, millis, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"PSetExRequest($id, $key, $millis, $value)"

}

object PSetExRequest {

  def apply[A](id: UUID, key: String, expires: FiniteDuration, value: A)(implicit s: Show[A]): PSetExRequest =
    new PSetExRequest(id, key, expires, s.show(value))

  def unapply(self: PSetExRequest): Option[(UUID, String, FiniteDuration, String)] =
    Some((self.id, self.key, self.millis, self.value))

  def create[A](id: UUID, key: String, expires: FiniteDuration, value: A, s: Show[A]): PSetExRequest =
    new PSetExRequest(id, key, expires, s.show(value))

}

sealed trait PSetExResponse                                                    extends CommandResponse
final case class PSetExSuspended(id: UUID, requestId: UUID)                    extends PSetExResponse
final case class PSetExSucceeded(id: UUID, requestId: UUID)                    extends PSetExResponse
final case class PSetExFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends PSetExResponse
