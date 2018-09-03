package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import cats.Show
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr }
import fastparse.all._

import scala.concurrent.duration.FiniteDuration

final class SetExRequest(val id: UUID, val key: String, val expires: FiniteDuration, val value: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = SetExResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("SETEX", Some(key), Some(expires.toSeconds.toString), Some(value))

  override protected lazy val responseParser: P[Expr] = fastParse(simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (SimpleExpr(OK), next) =>
      (SetExSucceeded(UUID.randomUUID(), id), next)
    case (SimpleExpr(QUEUED), next) =>
      (SetExSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SetExFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: SetExRequest =>
      id == that.id &&
      key == that.key &&
      expires == that.expires &&
      value == that.value
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, expires, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"SetExRequest($id, $key, $expires, $value)"

}

object SetExRequest {

  def apply[A](id: UUID, key: String, expires: FiniteDuration, value: A)(implicit s: Show[A]): SetExRequest =
    new SetExRequest(id, key, expires, s.show(value))

  def unapply(self: SetExRequest): Option[(UUID, String, FiniteDuration, String)] =
    Some((self.id, self.key, self.expires, self.value))

  def create[A](id: UUID, key: String, expires: FiniteDuration, value: A, s: Show[A]): SetExRequest =
    new SetExRequest(id, key, expires, s.show(value))

}

sealed trait SetExResponse                                                    extends CommandResponse
final case class SetExSuspended(id: UUID, requestId: UUID)                    extends SetExResponse
final case class SetExSucceeded(id: UUID, requestId: UUID)                    extends SetExResponse
final case class SetExFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetExResponse
