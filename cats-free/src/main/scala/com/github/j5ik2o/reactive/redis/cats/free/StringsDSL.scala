package com.github.j5ik2o.reactive.redis.cats.free

import cats.free._
import cats.~>
import com.github.j5ik2o.reactive.redis.Options.StartAndEnd
import com.github.j5ik2o.reactive.redis.RedisFutureClient
import com.github.j5ik2o.reactive.redis.StringsOperations.{ BitFieldRequest, BitOpRequest }

import scala.concurrent.{ ExecutionContext, Future }

trait StringsDSL[+A] extends RedisDSL[A]

trait StringsFreeFeature {

  // --- APPEND

  case class Append(key: String, value: String) extends StringsDSL[Option[Int]]

  // --- BITCOUNT

  case class BitCount(key: String, startAndEnd: Option[StartAndEnd] = None) extends StringsDSL[Option[Int]]

  // --- BITFIELD

  case class BitField(key: String, options: BitFieldRequest.SubOption*) extends StringsDSL[Option[Seq[Int]]]

  // --- BITOP

  case class BitOp(operand: BitOpRequest.Operand, outputKey: String, inputKey1: String, inputKey2: String)
      extends StringsDSL[Option[Int]]

  // --- BITPOS

  case class BitPos(key: String, bit: Int, startAndEnd: Option[StartAndEnd] = None) extends StringsDSL[Option[Int]]

  // --- DECR

  case class Descr(key: String) extends StringsDSL[Option[Int]]

  // --- DECRBY
  case class DescrBy(key: String, value: Int) extends StringsDSL[Option[Int]]

  // --- GET

  case class Get(key: String) extends StringsDSL[Option[String]]

  // --- GETBIT

  case class GetBit(key: String, offset: Int) extends StringsDSL[Option[Int]]

  // --- GETRANGE

  case class GetRange(key: String, startAndEnd: StartAndEnd) extends StringsDSL[Option[String]]

  // --- GETSET

  case class GetSet(key: String, value: String) extends StringsDSL[Option[String]]

  // --- SET

  case class Set(key: String, value: String) extends StringsDSL[Unit]

  def append(key: String, value: String): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](Append(key, value))

  def bitCount(key: String, startAndEnd: Option[StartAndEnd] = None): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](BitCount(key, startAndEnd))

  def bitField(key: String, options: BitFieldRequest.SubOption*): Free[StringsDSL, Option[Seq[Int]]] =
    Free.liftF[StringsDSL, Option[Seq[Int]]](BitField(key, options: _*))

  def bitOp(operand: BitOpRequest.Operand,
            outputKey: String,
            inputKey1: String,
            inputKey2: String): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](BitOp(operand, outputKey, inputKey1, inputKey2))

  def bitPos(key: String, bit: Int, startAndEnd: Option[StartAndEnd] = None): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](BitPos(key, bit, startAndEnd))

  def descr(key: String): Free[StringsDSL, Option[Int]] = Free.liftF[StringsDSL, Option[Int]](Descr(key))

  def descrBy(key: String, value: Int): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](DescrBy(key, value))

  def get(key: String): Free[StringsDSL, Option[String]] = Free.liftF[StringsDSL, Option[String]](Get(key))

  def getBit(key: String, offset: Int): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](GetBit(key, offset))

  def getRange(key: String, startAndEnd: StartAndEnd): Free[StringsDSL, Option[String]] =
    Free.liftF[StringsDSL, Option[String]](GetRange(key, startAndEnd))

  def getSet(key: String, value: String): Free[StringsDSL, Option[String]] =
    Free.liftF[StringsDSL, Option[String]](GetSet(key, value))

  def set(key: String, value: String): Free[StringsDSL, Unit] = Free.liftF[StringsDSL, Unit](Set(key, value))

  protected class StringsInterpreter(redisFutureClient: RedisFutureClient)(implicit ec: ExecutionContext)
      extends (StringsDSL ~> Future) {

    override def apply[A](fa: StringsDSL[A]): Future[A] = fa match {
      case Append(key, value) =>
        redisFutureClient.append(key, value).asInstanceOf[Future[A]]
      case BitCount(key, startAndEnd) =>
        redisFutureClient.bitCount(key, startAndEnd).asInstanceOf[Future[A]]
      case BitField(key, options) =>
        redisFutureClient.bitField(key, options).asInstanceOf[Future[A]]
      case BitOp(operand, outputKey, inputKey1, inputKey2) =>
        redisFutureClient.bitOp(operand, outputKey, inputKey1, inputKey2).asInstanceOf[Future[A]]
      case BitPos(key, bit, startAndEnd) =>
        redisFutureClient.bitPos(key, bit, startAndEnd).asInstanceOf[Future[A]]
      case Descr(key) =>
        redisFutureClient.descr(key).asInstanceOf[Future[A]]
      case DescrBy(key, value) =>
        redisFutureClient.descrBy(key, value).asInstanceOf[Future[A]]
      case Get(key) =>
        redisFutureClient.get(key).asInstanceOf[Future[A]]
      case GetBit(key, offset) =>
        redisFutureClient.getBit(key, offset).asInstanceOf[Future[A]]
      case GetRange(key, startAndEnd) =>
        redisFutureClient.getRange(key, startAndEnd).asInstanceOf[Future[A]]
      case GetSet(key, value) =>
        redisFutureClient.getSet(key, value).asInstanceOf[Future[A]]
      case Set(key, value) =>
        redisFutureClient.set(key, value).asInstanceOf[Future[A]]
    }

  }
}
