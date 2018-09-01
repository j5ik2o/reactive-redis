package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import cats.data.NonEmptyList
import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

final class MGetRequest(val id: UUID, val keys: NonEmptyList[String]) extends CommandRequest with StringParsersSupport {

  override type Response = MGetResponse

  override val isMasterOnly: Boolean = false

  override def asString: String = cs("MGET", keys.toList.map(Some(_)): _*)

  override protected lazy val responseParser: P[Expr] = fastParse(
    stringArrayReply | simpleStringReply | errorReply
  )

  override protected lazy val parseResponse: Handler = {
    case (ArrayExpr(values), next) =>
      (MGetSucceeded(UUID.randomUUID(), id, values.asInstanceOf[Seq[StringExpr]].map(_.value)), next)
    case (SimpleExpr(QUEUED), next) =>
      (MGetSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (MGetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: MGetRequest =>
      id == that.id &&
      keys == that.keys
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, keys)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"MGetRequest($id, $keys)"

}

object MGetRequest {

  def apply(id: UUID, keys: NonEmptyList[String]): MGetRequest = new MGetRequest(id, keys)
  def apply(id: UUID, key: String, keys: String*): MGetRequest = apply(id, NonEmptyList.of(key, keys: _*))

  def unapply(self: MGetRequest): Option[(UUID, NonEmptyList[String])] = Some((self.id, self.keys))

  def create(id: UUID, keys: NonEmptyList[String]): MGetRequest = new MGetRequest(id, keys)
  def create(id: UUID, key: String, keys: String*): MGetRequest = apply(id, NonEmptyList.of(key, keys: _*))

}

sealed trait MGetResponse                                                      extends CommandResponse
final case class MGetSuspended(id: UUID, requestId: UUID)                      extends MGetResponse
final case class MGetSucceeded(id: UUID, requestId: UUID, values: Seq[String]) extends MGetResponse
final case class MGetFailed(id: UUID, requestId: UUID, ex: Exception)          extends MGetResponse
