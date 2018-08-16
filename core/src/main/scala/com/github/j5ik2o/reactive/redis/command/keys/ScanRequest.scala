package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.keys.ScanRequest._
import com.github.j5ik2o.reactive.redis.command.keys.ScanSucceeded.ScanResult
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

final case class ScanRequest(id: UUID,
                             cursor: String,
                             matchOption: Option[MatchOption] = None,
                             countOption: Option[CountOption] = None)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = ScanResponse
  override val isMasterOnly: Boolean = false

  override def asString: String = {
    cs("SCAN",
       Some(cursor) :: matchOption.map(_.toList.map(Some(_))).getOrElse(Nil)
       ++ countOption.map(_.toList.map(Some(_))).getOrElse(Nil): _*)
  }

  override protected def responseParser: P[Expr] = fastParse(scan | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (ScanExpr(cursor, values), next) =>
      (ScanSucceeded(UUID.randomUUID(), id, ScanResult(cursor, values)), next)
    case (SimpleExpr(QUEUED), next) =>
      (ScanSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (ScanFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

}

object ScanRequest {

  final case class MatchOption(pattern: String) {
    protected[keys] def toList: List[String] = List("MATCH", pattern)
  }
  final case class CountOption(count: Int) {
    protected[keys] def toList: List[String] = List("COUNT", count.toString)
  }

}

sealed trait ScanResponse extends CommandResponse

object ScanSucceeded {
  final case class ScanResult(cursor: Option[String], values: Seq[String])
}
final case class ScanSucceeded(id: UUID, requestId: UUID, result: ScanResult) extends ScanResponse
final case class ScanSuspended(id: UUID, requestId: UUID)                     extends ScanResponse
final case class ScanFailed(id: UUID, requestId: UUID, ex: RedisIOException)  extends ScanResponse
