package com.github.j5ik2o.reactive.redis.command.hashes

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class HDelRequest(val id: UUID, val key: String, val fields: NonEmptyList[String])
    extends CommandRequest
    with StringParsersSupport {

  override type Response = HDelResponse
  override val isMasterOnly: Boolean = true

  override def asString: String =
    cs("HDEL", Some(key) :: fields.map(Some(_)).toList: _*)

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (HDelSucceeded(UUID.randomUUID(), id, n), next)
    case (SimpleExpr(QUEUED), next) =>
      (HDelSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (HDelFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: HDelRequest =>
      id == that.id &&
      key == that.key &&
      fields == that.fields
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, fields)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"HDelRequest($id, $key, $fields)"

}

object HDelRequest {

  def apply(id: UUID, key: String, fields: NonEmptyList[String]): HDelRequest = new HDelRequest(id, key, fields)

  def apply(id: UUID, key: String, field: String, fields: String*): HDelRequest =
    apply(id, key, NonEmptyList.of(field, fields: _*))

  def unapply(self: HDelRequest): Option[(UUID, String, NonEmptyList[String])] = Some((self.id, self.key, self.fields))

  def create(id: UUID, key: String, fields: NonEmptyList[String]): HDelRequest = apply(id, key, fields)

  def create(id: UUID, key: String, field: String, fields: String*): HDelRequest = apply(id, key, field, fields: _*)

}

sealed trait HDelResponse                                                      extends CommandResponse
final case class HDelSuspended(id: UUID, requestId: UUID)                      extends HDelResponse
final case class HDelSucceeded(id: UUID, requestId: UUID, numberDeleted: Long) extends HDelResponse
final case class HDelFailed(id: UUID, requestId: UUID, ex: RedisIOException)   extends HDelResponse
