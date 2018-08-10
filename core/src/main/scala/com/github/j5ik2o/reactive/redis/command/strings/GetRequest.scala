package com.github.j5ik2o.reactive.redis.command.strings

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringOptExpr }
import fastparse.all._

final case class GetRequest(id: UUID, key: String) extends CommandRequest with StringParsersSupport {

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

}

trait GetResponse                                                               extends CommandResponse
final case class GetSuspended(id: UUID, requestId: UUID)                        extends GetResponse
final case class GetSucceeded(id: UUID, requestId: UUID, value: Option[String]) extends GetResponse
final case class GetFailed(id: UUID, requestId: UUID, ex: RedisIOException)     extends GetResponse
