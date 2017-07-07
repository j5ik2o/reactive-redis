package com.github.j5ik2o.reactive.redis.cats.free

import cats.free._
import cats.~>
import com.github.j5ik2o.reactive.redis.Options.StartAndEnd
import com.github.j5ik2o.reactive.redis.RedisFutureClient
import com.github.j5ik2o.reactive.redis.StringsOperations.{ BitFieldRequest, BitOpRequest }

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }

trait StringsDSL[+A] extends RedisDSL[A]

trait StringsFreeFeature {

  // --- APPEND

  case class Append(key: String, value: String) extends StringsDSL[Option[Int]]

  def append(key: String, value: String): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](Append(key, value))

  // --- BITCOUNT

  case class BitCount(key: String, startAndEnd: Option[StartAndEnd] = None) extends StringsDSL[Option[Int]]

  def bitCount(key: String, startAndEnd: Option[StartAndEnd] = None): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](BitCount(key, startAndEnd))

  // --- BITFIELD

  case class BitField(key: String, options: BitFieldRequest.SubOption*) extends StringsDSL[Option[Seq[Int]]]

  def bitField(key: String, options: BitFieldRequest.SubOption*): Free[StringsDSL, Option[Seq[Int]]] =
    Free.liftF[StringsDSL, Option[Seq[Int]]](BitField(key, options: _*))

  // --- BITOP

  case class BitOp(operand: BitOpRequest.Operand, outputKey: String, inputKey1: String, inputKey2: String)
      extends StringsDSL[Option[Int]]

  def bitOp(operand: BitOpRequest.Operand,
            outputKey: String,
            inputKey1: String,
            inputKey2: String): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](BitOp(operand, outputKey, inputKey1, inputKey2))

  // --- BITPOS

  case class BitPos(key: String, bit: Int, startAndEnd: Option[StartAndEnd] = None) extends StringsDSL[Option[Int]]

  def bitPos(key: String, bit: Int, startAndEnd: Option[StartAndEnd] = None): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](BitPos(key, bit, startAndEnd))

  // --- DECR

  case class Descr(key: String) extends StringsDSL[Option[Int]]

  def descr(key: String): Free[StringsDSL, Option[Int]] = Free.liftF[StringsDSL, Option[Int]](Descr(key))

  // --- DECRBY

  case class DescrBy(key: String, value: Int) extends StringsDSL[Option[Int]]

  def descrBy(key: String, value: Int): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](DescrBy(key, value))

  // --- GET

  case class Get(key: String) extends StringsDSL[Option[String]]

  def get(key: String): Free[StringsDSL, Option[String]] = Free.liftF[StringsDSL, Option[String]](Get(key))

  // --- GETBIT

  case class GetBit(key: String, offset: Int) extends StringsDSL[Option[Int]]

  def getBit(key: String, offset: Int): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](GetBit(key, offset))

  // --- GETRANGE

  case class GetRange(key: String, startAndEnd: StartAndEnd) extends StringsDSL[Option[String]]

  def getRange(key: String, startAndEnd: StartAndEnd): Free[StringsDSL, Option[String]] =
    Free.liftF[StringsDSL, Option[String]](GetRange(key, startAndEnd))

  // --- GETSET

  case class GetSet(key: String, value: String) extends StringsDSL[Option[String]]

  def getSet(key: String, value: String): Free[StringsDSL, Option[String]] =
    Free.liftF[StringsDSL, Option[String]](GetSet(key, value))

  // --- INCR

  case class Incr(key: String) extends StringsDSL[Option[Int]]

  def incr(key: String): Free[StringsDSL, Option[Int]] = Free.liftF[StringsDSL, Option[Int]](Incr(key))

  // --- INCRBY

  case class IncrBy(key: String, value: Int) extends StringsDSL[Option[Int]]

  def incrBy(key: String, value: Int): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](IncrBy(key, value))

  // --- INCRBYFLOAT

  case class IncrByFloat(key: String, value: Double) extends StringsDSL[Option[Double]]

  def incrByFloat(key: String, value: Double): Free[StringsDSL, Option[Double]] =
    Free.liftF[StringsDSL, Option[Double]](IncrByFloat(key, value))

  // --- MGET

  case class MGet(keys: Seq[String]) extends StringsDSL[Option[Seq[Option[String]]]]

  def mGet(keys: Seq[String]): Free[StringsDSL, Option[Seq[Option[String]]]] =
    Free.liftF[StringsDSL, Option[Seq[Option[String]]]](MGet(keys))

  // --- MSET

  case class MSet(values: Map[String, Any]) extends StringsDSL[Unit]

  def mSet(values: Map[String, Any]): Free[StringsDSL, Unit] = Free.liftF[StringsDSL, Unit](MSet(values))

  // --- MSETNX

  case class MSetNx(values: Map[String, Any]) extends StringsDSL[Option[Boolean]]

  def mSetNx(values: Map[String, Any]): Free[StringsDSL, Option[Boolean]] =
    Free.liftF[StringsDSL, Option[Boolean]](MSetNx(values))

  // --- PSETEX

  case class PSetEx(key: String, duration: FiniteDuration, value: String) extends StringsDSL[Unit]

  def pSetEx(key: String, duration: FiniteDuration, value: String): Free[StringsDSL, Unit] =
    Free.liftF[StringsDSL, Unit](PSetEx(key, duration, value))

  // --- SET

  case class Set(key: String, value: String) extends StringsDSL[Unit]

  def set(key: String, value: String): Free[StringsDSL, Unit] = Free.liftF[StringsDSL, Unit](Set(key, value))

  // --- SETBIT

  case class SetBit(key: String, offset: Int, value: Int) extends StringsDSL[Option[Int]]

  def setBit(key: String, offset: Int, value: Int): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](SetBit(key, offset, value))

  // --- SETEX

  case class SetEx(key: String, expires: FiniteDuration, value: String) extends StringsDSL[Unit]

  def setEx(key: String, expires: FiniteDuration, value: String): Free[StringsDSL, Unit] =
    Free.liftF[StringsDSL, Unit](SetEx(key, expires, value))

  // --- SETNX

  case class SetNx(key: String, value: String) extends StringsDSL[Option[Boolean]]

  def setNx(key: String, value: String): Free[StringsDSL, Option[Boolean]] =
    Free.liftF[StringsDSL, Option[Boolean]](SetNx(key, value))

  // --- SETRANGE

  case class SetRange(key: String, range: Int, value: String) extends StringsDSL[Option[Int]]

  def setRange(key: String, range: Int, value: String): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](SetRange(key, range, value))

  // --- STRLEN

  case class StrLen(key: String) extends StringsDSL[Option[Int]]

  def strlen(key: String): Free[StringsDSL, Option[Int]] = Free.liftF[StringsDSL, Option[Int]](StrLen(key))

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
      case Incr(key) =>
        redisFutureClient.incr(key).asInstanceOf[Future[A]]
      case IncrBy(key, value) =>
        redisFutureClient.incrBy(key, value).asInstanceOf[Future[A]]
      case IncrByFloat(key, value) =>
        redisFutureClient.incrByFloat(key, value).asInstanceOf[Future[A]]
      case MGet(keys) =>
        redisFutureClient.mGet(keys).asInstanceOf[Future[A]]
      case MSet(values) =>
        redisFutureClient.mSet(values).asInstanceOf[Future[A]]
      case MSetNx(values) =>
        redisFutureClient.mSetNx(values).asInstanceOf[Future[A]]
      case PSetEx(key, duration, value) =>
        redisFutureClient.pSetEx(key, duration, value).asInstanceOf[Future[A]]
      case Set(key, value) =>
        redisFutureClient.set(key, value).asInstanceOf[Future[A]]
      case SetBit(key, offset, value) =>
        redisFutureClient.setBit(key, offset, value).asInstanceOf[Future[A]]
      case SetEx(key, expires, value) =>
        redisFutureClient.setEx(key, expires, value).asInstanceOf[Future[A]]
      case SetNx(key, value) =>
        redisFutureClient.setNx(key, value).asInstanceOf[Future[A]]
      case SetRange(key, range, value) =>
        redisFutureClient.setRange(key, range, value).asInstanceOf[Future[A]]
      case StrLen(key) =>
        redisFutureClient.strlen(key).asInstanceOf[Future[A]]
    }

  }
}
