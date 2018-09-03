package com.github.j5ik2o.reactive.redis.command.sets
import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class SAddRequest(val id: UUID, val key: String, val values: NonEmptyList[String])
    extends CommandRequest
    with StringParsersSupport {

  override type Response = SAddResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = cs("SADD", Some(key) :: values.map(Some(_)).toList: _*)

  override protected def responseParser: P[Expr] = fastParse(integerReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (SAddSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (SAddSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SAddFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: SAddRequest =>
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

  override def toString: String = s"SAddRequest($id, $key, $values)"
}

object SAddRequest {

  def apply(id: UUID, key: String, value: String, values: String*): SAddRequest =
    new SAddRequest(id, key, NonEmptyList.of(value, values: _*))

  def apply(id: UUID, key: String, values: NonEmptyList[String]): SAddRequest = new SAddRequest(id, key, values)

  def create(id: UUID, key: String, value: String, values: String*): SAddRequest = apply(id, key, value, values: _*)

  def create(id: UUID, key: String, values: NonEmptyList[String]): SAddRequest = apply(id, key, values)

}

sealed trait SAddResponse                                                    extends CommandResponse
final case class SAddSucceeded(id: UUID, requestId: UUID, value: Long)       extends SAddResponse
final case class SAddSuspended(id: UUID, requestId: UUID)                    extends SAddResponse
final case class SAddFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SAddResponse
