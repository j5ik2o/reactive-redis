package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }
import fastparse.all._

final class GetRequest(val id: UUID, val key: String) extends CommandRequest with StringParsersSupport {

  override type Response = GetResponse

  override val isMasterOnly: Boolean = false

  override def asString: String = cs("GET", Some(key))

  override protected lazy val responseParser: P[Expr] = fastParse(bulkStringReply | simpleStringReply | errorReply)
  // override protected lazy val responseParser: P[Expr] = default(Reference)(NewParsers.getParer(Reference))

  override protected lazy val parseResponse: Handler = {
    case (StringOptExpr(s), next) =>
      (GetSucceeded(UUID.randomUUID(), id, s), next)
    case (SimpleExpr(QUEUED), next) =>
      (GetSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (GetFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: GetRequest =>
      id == that.id &&
      key == that.key
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, key)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"GetRequest($id, $key)"

}

object GetRequest {

  def apply(id: UUID, key: String): GetRequest = new GetRequest(id, key)

  def unapply(self: GetRequest): Option[(UUID, String)] = Some((self.id, self.key))

  def create(id: UUID, key: String): GetRequest = apply(id, key)

}

trait GetResponse                                                               extends CommandResponse
final case class GetSuspended(id: UUID, requestId: UUID)                        extends GetResponse
final case class GetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetResponse
final case class GetFailed(id: UUID, requestId: UUID, ex: RedisIOException)     extends GetResponse
