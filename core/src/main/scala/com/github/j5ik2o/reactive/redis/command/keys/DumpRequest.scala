package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse }
import com.github.j5ik2o.reactive.redis.parser.ByteParsers._
import com.github.j5ik2o.reactive.redis.parser.model.{ BytesOptExpr, ErrorExpr, Expr, SimpleExpr }
import fastparse.byte.all._

case class DumpRequest(id: UUID, key: String) extends CommandRequest {
  type Elem              = Byte
  type Repr              = Bytes
  override type Response = DumpResponse

  override val isMasterOnly: Boolean = false

  override def asString: String = s"DUMP $key"

  override protected def responseParser: P[Expr] = P(bulkBytesReply | simpleStringReply)

  override protected def convertToParseSource(s: Bytes): Bytes = s

  override protected def parseResponse: Handler = {
    case (BytesOptExpr(b), next) =>
      (DumpSucceeded(UUID.randomUUID(), id, b), next)
    case (SimpleExpr(QUEUED), next) =>
      (DumpSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (DumpFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

sealed trait DumpResponse                                                       extends CommandResponse
case class DumpSuspended(id: UUID, requestId: UUID)                             extends DumpResponse
case class DumpSucceeded(id: UUID, requestId: UUID, value: Option[Array[Byte]]) extends DumpResponse
case class DumpFailed(id: UUID, requestId: UUID, ex: RedisIOException)          extends DumpResponse
