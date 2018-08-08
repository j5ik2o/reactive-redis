package com.github.j5ik2o.reactive.redis.command.lists
import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ ErrorExpr, Expr, SimpleExpr, StringExpr }

import scala.concurrent.duration.Duration

final case class BRPopLPushRequest(id: UUID, source: String, destination: String, timeout: Duration)
    extends CommandRequest
    with StringParsersSupport {
  override type Response = BRPopLPushResponse
  override val isMasterOnly: Boolean = true

  override def asString: String = s"BRPOPLPUSH $source $destination ${timeout.toSeconds}"

  override protected def responseParser: P[Expr] = fastParse(bulkStringReply)
  override protected def parseResponse: Handler = {
    case (StringExpr(v), next) =>
      (BRPopLPushSucceeded(UUID.randomUUID(), id, v), next)
    case (SimpleExpr(QUEUED), next) =>
      (BRPopLPushSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (BRPopLPushFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait BRPopLPushResponse                                                    extends CommandResponse
final case class BRPopLPushSucceeded(id: UUID, requestId: UUID, value: String)     extends BRPopLPushResponse
final case class BRPopLPushSuspended(id: UUID, requestId: UUID)                    extends BRPopLPushResponse
final case class BRPopLPushFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends BRPopLPushResponse
