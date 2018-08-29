package com.github.j5ik2o.reactive.redis.command.hashes

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

final class HGetAllRequest(val id: UUID, val key: String) extends CommandRequest with StringParsersSupport {

  override type Response = HGetAllResponse
  override val isMasterOnly: Boolean = false

  override def asString: String = cs("HGETALL", Some(key))

  override protected lazy val responseParser: P[Expr] = fastParse(stringArrayReply | simpleStringReply)

  override protected lazy val parseResponse: Handler = {
    case (ArrayExpr(values), next) =>
      (HGetAllSucceeded(UUID.randomUUID(), id, values.asInstanceOf[Seq[StringExpr]].map(_.value)), next)
    case (SimpleExpr(QUEUED), next) =>
      (HGetAllSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (HGetAllFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: HGetAllRequest =>
      id == that.id &&
      key == that.key
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"HGetAllRequest($id, $key)"
}

object HGetAllRequest {

  def apply(id: UUID, key: String): HGetAllRequest = new HGetAllRequest(id, key)

  def unapply(self: HGetAllRequest): Option[(UUID, String)] = Some((self.id, self.key))

  def create(id: UUID, key: String): HGetAllRequest = apply(id, key)

}

sealed trait HGetAllResponse                                                      extends CommandResponse
final case class HGetAllSuspended(id: UUID, requestId: UUID)                      extends HGetAllResponse
final case class HGetAllSucceeded(id: UUID, requestId: UUID, values: Seq[String]) extends HGetAllResponse
final case class HGetAllFailed(id: UUID, requestId: UUID, ex: RedisIOException)   extends HGetAllResponse
