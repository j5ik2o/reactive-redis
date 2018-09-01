package com.github.j5ik2o.reactive.redis.command.keys

import java.util.UUID

import com.github.j5ik2o.reactive.redis.RedisIOException
import com.github.j5ik2o.reactive.redis.command.keys.SortResponse._
import com.github.j5ik2o.reactive.redis.command.{ CommandRequest, CommandResponse, StringParsersSupport }
import com.github.j5ik2o.reactive.redis.parser.StringParsers._
import com.github.j5ik2o.reactive.redis.parser.model._
import enumeratum._
import fastparse.all._

import scala.collection.immutable

final class SortRequest(val id: UUID,
                        val key: String,
                        val byPattern: Option[ByPattern] = None,
                        val limitOffset: Option[LimitOffset] = None,
                        val getPatterns: Seq[GetPattern] = Seq.empty,
                        val order: Option[Order] = None,
                        val alpha: Boolean = false,
                        val store: Option[Store] = None)
    extends CommandRequest
    with StringParsersSupport {

  override type Response = SortResponse
  override val isMasterOnly: Boolean = true

  override def asString: String =
    cs(
      "SORT",
      Some(key) ::
      byPattern.map(_.toList.map(Some(_))).getOrElse(Nil) ++
      limitOffset.map(_.toList.map(Some(_))).getOrElse(Nil) ++
      getPatterns.flatMap(_.toList.map(Some(_))) ++
      order.map(v => List(Some(v.entryName))).getOrElse(Nil) ++
      (if (alpha) List(Some("ALPHA")) else Nil) ++
      store.map(_.toList.map(Some(_))).getOrElse(Nil): _*
    )

  override protected def responseParser: P[Expr] =
    fastParse(stringOptArrayReply | integerReply | simpleStringReply | errorReply)

  override protected def parseResponse: Handler = {
    case (NumberExpr(n), next) =>
      (SortLongSucceeded(UUID.randomUUID(), id, n), next)
    case (ArrayExpr(values), next) =>
      (SortListSucceeded(UUID.randomUUID(), id, values.asInstanceOf[Seq[StringOptExpr]].map(_.value)), next)
    case (SimpleExpr(QUEUED), next) =>
      (SortSuspended(UUID.randomUUID(), id), next)
    case (ErrorExpr(msg), next) =>
      (SortFailed(UUID.randomUUID(), id, RedisIOException(Some(msg))), next)
  }

  override def equals(other: Any): Boolean = other match {
    case that: SortRequest =>
      id == that.id &&
      key == that.key &&
      byPattern == that.byPattern &&
      limitOffset == that.limitOffset &&
      getPatterns == that.getPatterns &&
      order == that.order &&
      alpha == that.alpha &&
      store == that.store
    case _ => false
  }
  override def hashCode(): Int = {
    val state = Seq(id, key, byPattern, limitOffset, getPatterns, order, alpha, store)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString: String =
    s"SortRequest($id, $key, $byPattern, $limitOffset, $getPatterns, $order, $alpha, $store)"
}

object SortRequest {

  def apply(id: UUID,
            key: String,
            byPattern: Option[ByPattern] = None,
            limitOffset: Option[LimitOffset] = None,
            getPatterns: Seq[GetPattern] = Seq.empty,
            order: Option[Order] = None,
            alpha: Boolean = false,
            store: Option[Store] = None): SortRequest =
    new SortRequest(id, key, byPattern, limitOffset, getPatterns, order, alpha, store)

  def unapply(self: SortRequest): Option[
    (UUID, String, Option[ByPattern], Option[LimitOffset], Seq[GetPattern], Option[Order], Boolean, Option[Store])
  ] =
    Some((self.id, self.key, self.byPattern, self.limitOffset, self.getPatterns, self.order, self.alpha, self.store))

  def create(id: UUID,
             key: String,
             byPattern: Option[ByPattern] = None,
             limitOffset: Option[LimitOffset] = None,
             getPatterns: Seq[GetPattern] = Seq.empty,
             order: Option[Order] = None,
             alpha: Boolean = false,
             store: Option[Store] = None): SortRequest =
    apply(id, key, byPattern, limitOffset, getPatterns, order, alpha, store)

}

object SortResponse {

  final case class ByPattern(pattern: String) {
    def toList: List[String] = List("BY", pattern)
  }

  final case class LimitOffset(offset: Int, count: Int) {
    def toList: List[String] = List("LIMIT", offset.toString, count.toString)
  }

  final case class GetPattern(pattern: String) {
    def toList: List[String] = List("GET", pattern)
  }

  sealed abstract class Order(override val entryName: String) extends EnumEntry

  object Order extends Enum[Order] {
    override def values: immutable.IndexedSeq[Order] = findValues
    final case object Asc  extends Order("ASC")
    final case object Desc extends Order("DESC")
  }

  final case class Store(destination: String) {
    def toList: List[String] = List("STORE", destination)
  }

}

sealed trait SortResponse                                                  extends CommandResponse
sealed abstract class SortSucceeded(id: UUID, requestId: UUID)             extends SortResponse
final case class SortLongSucceeded(id: UUID, requestId: UUID, value: Long) extends SortSucceeded(id, requestId)
final case class SortListSucceeded(id: UUID, requestId: UUID, values: Seq[Option[String]])
    extends SortSucceeded(id, requestId)
final case class SortSuspended(id: UUID, requestId: UUID)                    extends SortResponse
final case class SortFailed(id: UUID, requestId: UUID, ex: RedisIOException) extends SortResponse
