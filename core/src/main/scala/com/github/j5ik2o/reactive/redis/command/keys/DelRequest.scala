package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class DelRequest(val id: UUID, val keys: NonEmptyList[String]) extends CommandRequest with StringParsersSupport {

  override type Response = DelResponse

  override val isMasterOnly: Boolean = true

  override def asString: String = cs("DEL", keys.map(Some(_)).toList: _*)

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (DelSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (DelSuspended(id, id), next)
    case (ErrorExpr(msg), next) =>
      (DelFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: DelRequest =>
      id == that.id &&
      keys == that.keys
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, keys)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"DelRequest($id, $keys)"

}

object DelRequest {

  def apply(id: UUID, key: String, keys: String*): DelRequest = new DelRequest(id, NonEmptyList.of(key, keys: _*))

  def apply(id: UUID, keys: NonEmptyList[String]): DelRequest = new DelRequest(id, keys)

  def unapply(self: DelRequest): Option[(UUID, NonEmptyList[String])] = Some((self.id, self.keys))

  def create(id: UUID, key: String, keys: String*): DelRequest = apply(id, key, keys: _*)

  def create(id: UUID, keys: NonEmptyList[String]): DelRequest = apply(id, keys)

}

sealed trait DelResponse                                                    extends CommandResponse
final case class DelSuspended(id: UUID, requestId: UUID)                    extends DelResponse
final case class DelSucceeded(id: UUID, requestId: UUID, value: Long)       extends DelResponse
final case class DelFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends DelResponse
