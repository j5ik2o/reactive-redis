package com.github.j5ik2o.reactive.redis.cats.free

import cats.free._
import cats.~>
import com.github.j5ik2o.reactive.redis.Options.StartAndEnd
import com.github.j5ik2o.reactive.redis.RedisFutureClient
import com.github.j5ik2o.reactive.redis.StringsOperations.{ BitFieldRequest, BitOpRequest }
import com.github.j5ik2o.reactive.redis.free.StringsAPIs

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ ExecutionContext, Future }

trait StringsFreeFeature extends StringsAPIs {

  // --- APPEND

  def append(key: String, value: String): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](Append(key, value))

  // --- BITCOUNT

  def bitCount(key: String, startAndEnd: Option[StartAndEnd] = None): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](BitCount(key, startAndEnd))

  // --- BITFIELD

  def bitField(key: String, options: BitFieldRequest.SubOption*): Free[StringsDSL, Option[Seq[Int]]] =
    Free.liftF[StringsDSL, Option[Seq[Int]]](BitField(key, options: _*))

  // --- BITOP

  def bitOp(operand: BitOpRequest.Operand,
            outputKey: String,
            inputKey1: String,
            inputKey2: String): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](BitOp(operand, outputKey, inputKey1, inputKey2))

  // --- BITPOS

  def bitPos(key: String, bit: Int, startAndEnd: Option[StartAndEnd] = None): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](BitPos(key, bit, startAndEnd))

  // --- DECR

  def descr(key: String): Free[StringsDSL, Option[Int]] = Free.liftF[StringsDSL, Option[Int]](Descr(key))

  // --- DECRBY

  def descrBy(key: String, value: Int): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](DescrBy(key, value))

  // --- GET

  def get(key: String): Free[StringsDSL, Option[String]] = Free.liftF[StringsDSL, Option[String]](Get(key))

  // --- GETBIT

  def getBit(key: String, offset: Int): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](GetBit(key, offset))

  // --- GETRANGE

  def getRange(key: String, startAndEnd: StartAndEnd): Free[StringsDSL, Option[String]] =
    Free.liftF[StringsDSL, Option[String]](GetRange(key, startAndEnd))

  // --- GETSET

  def getSet(key: String, value: String): Free[StringsDSL, Option[String]] =
    Free.liftF[StringsDSL, Option[String]](GetSet(key, value))

  // --- INCR

  def incr(key: String): Free[StringsDSL, Option[Int]] = Free.liftF[StringsDSL, Option[Int]](Incr(key))

  // --- INCRBY

  def incrBy(key: String, value: Int): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](IncrBy(key, value))

  // --- INCRBYFLOAT

  def incrByFloat(key: String, value: Double): Free[StringsDSL, Option[Double]] =
    Free.liftF[StringsDSL, Option[Double]](IncrByFloat(key, value))

  // --- MGET

  def mGet(keys: Seq[String]): Free[StringsDSL, Option[Seq[Option[String]]]] =
    Free.liftF[StringsDSL, Option[Seq[Option[String]]]](MGet(keys))

  // --- MSET

  def mSet(values: Map[String, Any]): Free[StringsDSL, Unit] = Free.liftF[StringsDSL, Unit](MSet(values))

  // --- MSETNX

  def mSetNx(values: Map[String, Any]): Free[StringsDSL, Option[Boolean]] =
    Free.liftF[StringsDSL, Option[Boolean]](MSetNx(values))

  // --- PSETEX

  def pSetEx(key: String, duration: FiniteDuration, value: String): Free[StringsDSL, Unit] =
    Free.liftF[StringsDSL, Unit](PSetEx(key, duration, value))

  // --- SET

  def set(key: String, value: String): Free[StringsDSL, Unit] = Free.liftF[StringsDSL, Unit](Set(key, value))

  // --- SETBIT

  def setBit(key: String, offset: Int, value: Int): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](SetBit(key, offset, value))

  // --- SETEX

  def setEx(key: String, expires: FiniteDuration, value: String): Free[StringsDSL, Unit] =
    Free.liftF[StringsDSL, Unit](SetEx(key, expires, value))

  // --- SETNX

  def setNx(key: String, value: String): Free[StringsDSL, Option[Boolean]] =
    Free.liftF[StringsDSL, Option[Boolean]](SetNx(key, value))

  // --- SETRANGE

  def setRange(key: String, range: Int, value: String): Free[StringsDSL, Option[Int]] =
    Free.liftF[StringsDSL, Option[Int]](SetRange(key, range, value))

  // --- STRLEN

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
