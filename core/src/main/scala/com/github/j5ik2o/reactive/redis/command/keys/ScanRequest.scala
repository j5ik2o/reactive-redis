package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.keys.ScanRequest._
import com.github.j5ik2o.reactive.redis.command.keys.ScanSucceeded.ScanResult
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.all._

final class ScanRequest(val id: UUID,
                        val cursor: String,
                        val matchOption: Option[MatchOption],
                        val countOption: Option[CountOption])
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

  override def equals(other: Any): Boolean = other match {
    case that: ScanRequest =>
      id == that.id &&
      cursor == that.cursor &&
      matchOption == that.matchOption &&
      countOption == that.countOption
    case _ => false
  }

  @SuppressWarnings(Array("org.wartremover.warts.JavaSerializable"))
  override def hashCode(): Int = {
    val state = Seq(id, cursor, matchOption, countOption)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String = s"ScanRequest($id, $cursor, $matchOption, $countOption)"

}

object ScanRequest {

  def apply(id: UUID,
            cursor: String,
            matchOption: Option[MatchOption] = None,
            countOption: Option[CountOption] = None): ScanRequest =
    new ScanRequest(id, cursor, matchOption, countOption)

  def unapply(self: ScanRequest): Option[(UUID, String, Option[MatchOption], Option[CountOption])] =
    Some((self.id, self.cursor, self.matchOption, self.countOption))

  def create(id: UUID,
             cursor: String,
             matchOption: Option[MatchOption] = None,
             countOption: Option[CountOption] = None): ScanRequest = apply(id, cursor, matchOption, countOption)

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
