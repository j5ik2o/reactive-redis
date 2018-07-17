package com.github.j5ik2o.reactive.redis.parser

import com.github.j5ik2o.reactive.redis.parser.model._
import fastparse.byte.all._

object ByteParsers {
  val QUEUED = "QUEUED"
  val OK     = "OK"

  private[parser] val crlf: P0 = P(BS('\r'.toByte, '\n'.toByte))

  private[parser] val digit: P0      = P(ByteIn(('0' to '9').map(_.toByte)))
  private[parser] val lowerAlpha: P0 = P(ByteIn(('a' to 'z').map(_.toByte)))
  private[parser] val upperAlpha: P0 = P(ByteIn(('A' to 'Z').map(_.toByte)))
  private[parser] val alpha: P0      = P(lowerAlpha | upperAlpha)
  private[parser] val alphaDigit: P0 = P(alpha | digit)

  private[parser] val error: P[ErrorExpr] =
    P(BS('-'.toByte) ~ (!crlf ~/ AnyByte).rep(1).!).map(v => ErrorExpr(v.decodeUtf8.right.get))

  private val length = P(BS('$'.toByte) ~ BS('-'.toByte).!.? ~ digit.rep(1).!).map {
    case (m, n) =>
      val r = LengthExpr(m.map(_ => -1).getOrElse(1) * n.decodeUtf8.right.get.toInt)
      println(r)
      r
  }
  private val simple: P[SimpleExpr] =
    P(BS('+'.toByte) ~ alphaDigit.rep(1).!).map(v => SimpleExpr(v.decodeUtf8.right.get))
  private val number: P[NumberExpr] = P(BS(':'.toByte) ~ BS('-'.toByte).!.? ~ alphaDigit.rep(1).!).map {
    case (minus, n) =>
      NumberExpr(minus.map(_ => -1).getOrElse(1) * n.decodeUtf8.right.get.toInt)
  }
  private val byteString: P[BytesExpr] = P((!crlf ~/ AnyByte).rep.!).map(v => BytesExpr(v.toArray))
  private val string: P[StringExpr]    = P((!crlf ~/ AnyByte).rep.!).map(v => StringExpr(v.decodeUtf8.right.get))
  private val arrayPrefix: P[Int]      = P(BS('*'.toByte) ~ digit.rep(1).!).map(_.decodeUtf8.right.get.toInt)

  private val errorWithCrLf: P[ErrorExpr] = P(error ~ crlf)

  private val simpleWithCrLf: P[SimpleExpr]         = P(simple ~ crlf)
  private val integerWithCrLf: P[NumberExpr]        = P(number ~ crlf)
  private val arrayPrefixWithCrlf: P[ArraySizeExpr] = P(arrayPrefix ~ crlf).map(ArraySizeExpr)

  private def array[A <: Expr](elementExpr: P[A]): P[ArrayExpr[A]] =
    P(arrayPrefixWithCrlf ~ elementExpr.rep(sep = crlf)).map {
      case (size, values) =>
        require(size.value == values.size)
        ArrayExpr(values)
    }

  private val bytesOptArrayElement: P[BytesOptExpr] = P(length ~ crlf ~ byteString.?).map {
    case (size, _) if size.value == -1 =>
      BytesOptExpr(None)
    case (size, value) =>
      BytesOptExpr(value.map(_.value))
  }

  private val stringOptArrayElement: P[StringOptExpr] = P(length ~ crlf ~ string.?).map {
    case (size, _) if size.value == -1 =>
      StringOptExpr(None)
    case (size, value) =>
      StringOptExpr(value.map(_.value))
  }

  private val integerArrayElement: P[NumberExpr] = P(number)

  private val stringOptArrayWithCrLf: P[ArrayExpr[StringOptExpr]]    = P(array(stringOptArrayElement) ~ crlf.?)
  private[parser] val integerArrayWithCrLf: P[ArrayExpr[NumberExpr]] = P(array(integerArrayElement) ~ crlf.?)

  private[parser] val bulkBytesWithCrLf: P[BytesOptExpr]   = P((length ~ crlf).flatMap(l => bulkBytesRest(l.value)))
  private[parser] val bulkStringWithCrLf: P[StringOptExpr] = P((length ~ crlf).flatMap(l => bulkStringRest(l.value)))

  private def bulkBytesRest(l: Int): P[BytesOptExpr] = {
    if (l == -1) {
      P(End).!.map(_ => BytesOptExpr(None))
    } else {
      P(byteString ~ crlf).map(v => BytesOptExpr(Some(v.value)))
    }
  }

  private def bulkStringRest(l: Int): P[StringOptExpr] = {
    if (l == -1) {
      P(End).!.map(_ => StringOptExpr(None))
    } else {
      P(string ~ crlf).map(v => StringOptExpr(Some(v.value)))
    }
  }

  val simpleStringReply: P[Expr]   = P(simpleWithCrLf | errorWithCrLf)
  val integerReply: P[Expr]        = P(integerWithCrLf | errorWithCrLf)
  val integerArrayReply: P[Expr]   = P(integerArrayWithCrLf | errorWithCrLf)
  val stringOptArrayReply: P[Expr] = P(stringOptArrayWithCrLf | errorWithCrLf)
  val bulkStringReply: P[Expr]     = P(bulkStringWithCrLf | errorWithCrLf)
  val bulkBytesReply: P[Expr]      = P(bulkBytesWithCrLf | errorWithCrLf)
}
