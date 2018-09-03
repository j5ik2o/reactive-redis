package com.github.j5ik2o.reactive.redis.command.lists

import java.util.UUID

import cats.Show
import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class LPushRequest(val id: UUID, val key: String, val values: NonEmptyList[String])
    extends CommandRequest
    with StringParsersSupport {

  override type Response = LPushResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("LPUSH", Some(key) :: values.map(Some(_)).toList: _*)

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (LPushSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (LPushSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (LPushFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: LPushRequest =>
      id == that.id &&
      key == that.key &&
      values == that.values
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, values)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"LPushRequest($id, $key, $values)"
}

object LPushRequest {

  def apply[A](id: UUID, key: String, value: A)(implicit s: Show[A]): LPushRequest =
    apply(id, key, NonEmptyList.of(value))

  def apply[A](id: UUID, key: String, values: NonEmptyList[A])(implicit s: Show[A]): LPushRequest =
    new LPushRequest(id, key, values.map(s.show))

  def unapply(self: LPushRequest): Option[(UUID, String, NonEmptyList[String])] = Some((self.id, self.key, self.values))

  def create[A](id: UUID, key: String, value: A, s: Show[A]): LPushRequest = apply(id, key, value)(s)

  def create[A](id: UUID, key: String, values: NonEmptyList[A])(implicit s: Show[A]): LPushRequest =
    apply(id, key, values)(s)

}

sealed trait LPushResponse                                                    extends CommandResponse
final case class LPushSuspended(id: UUID, requestId: UUID)                    extends LPushResponse
final case class LPushSucceeded(id: UUID, requestId: UUID, value: Long)       extends LPushResponse
final case class LPushFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends LPushResponse
