package com.github.j5ik2o.reactive.redis.command.hashes

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, NumberExpr, SimpleExpr }
import fastparse.all._

final class HExistsRequest(val id: UUID, val key: String, val field: String)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = HExistsResponse
  override val isMasterOnly: Boolean = false

  override def asString: String = cs("HEXISTS", Some(key), Some(field))

  override protected lazy val responseParser: P[Expr] = fastParse(integerReply | simpleStringReply)

  override protected lazy val parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (HExistsSucceeded(UUID.randomUUID(), id, n == 1), next)
    case (SimpleExpr(QUEUED), next) =>
      (HExistsSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (HExistsFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: HExistsRequest =>
      id == that.id &&
      key == that.key &&
      field == that.field
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key, field)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"HExistsRequest($id, $key, $field)"

}

object HExistsRequest {

  def apply(id: UUID, key: String, field: String): HExistsRequest = new HExistsRequest(id, key, field)

  def unapply(self: HExistsRequest): Option[(UUID, String, String)] = Some((self.id, self.key, self.field))

  def create(id: UUID, key: String, field: String): HExistsRequest = apply(id, key, field)

}

sealed trait HExistsResponse                                                    extends CommandResponse
final case class HExistsSuspended(id: UUID, requestId: UUID)                    extends HExistsResponse
final case class HExistsSucceeded(id: UUID, requestId: UUID, isExists: Boolean) extends HExistsResponse
final case class HExistsFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends HExistsResponse
