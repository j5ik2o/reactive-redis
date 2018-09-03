package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import cats.Show
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class SetNxRequest(val id: UUID, val key: String, val value: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = SetNxResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("SETNX", Some(key), Some(value))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (SetNxSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (SetNxSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SetNxFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: SetNxRequest =>
      id == that.id &&
      key == that.key &&
      value == that.value
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, value)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"SetNxRequest($id, $key, $value)"
}

object SetNxRequest {

  def apply[A](id: UUID, key: String, value: A)(implicit s: Show[A]): SetNxRequest =
    new SetNxRequest(id, key, s.show(value))

  def unapply(self: SetNxRequest): Option[(UUID, String, String)] = Some((self.id, self.key, self.value))

  def create[A](id: UUID, key: String, value: A, s: Show[A]): SetNxRequest =
    new SetNxRequest(id, key, s.show(value))

}

sealed trait SetNxResponse                                                    extends CommandResponse
final case class SetNxSuspended(id: UUID, requestId: UUID)                    extends SetNxResponse
final case class SetNxSucceeded(id: UUID, requestId: UUID, isSet: Boolean)    extends SetNxResponse
final case class SetNxFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SetNxResponse
