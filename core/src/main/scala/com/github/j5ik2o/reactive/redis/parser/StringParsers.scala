package com.github.j5ik2o.reactive.redis.parser

import com.github.j5ik2o.reactive.redis.parser.model._

@SuppressWarnings(
  Array("org.wartremover.warts.Product",
        "org.wartremover.warts.Serializable",
        "org.wartremover.warts.Equals",
        "org.wartremover.warts.EitherProjectionPartial")
)
object StringParsers {
  import fastparse.all._
  val QUEUED: String = "QUEUED"
  val OK: String     = "OK"
  val NOKEY: String  = "NOKEY"

  private val digit: P0      = P(CharIn('0' to '9'))
  private val lowerAlpha: P0 = P(CharIn('a' to 'z'))
  private val upperAlpha: P0 = P(CharIn('A' to 'Z'))
  private val alpha: P0      = P(lowerAlpha | upperAlpha)
  private val alphaDigit: P0 = P(alpha | digit)

  private val crlf: P0 = P("\r\n")

  private val length: P[LengthExpr] = P("$" ~/ "-".!.? ~/ digit.rep(1).!).map {
    case (m, n) =>
      LengthExpr(m.map(_ => -1).getOrElse(1) * n.toInt)
  }
  private val simple: P[SimpleExpr] = P("+" ~/ alphaDigit.rep(1).!).map(SimpleExpr)
  private val number: P[NumberExpr] = P(":" ~/ "-".!.? ~/ alphaDigit.rep(1).!).map {
    case (minus, n) =>
      NumberExpr(minus.map(_ => -1).getOrElse(1) * n.toInt)
  }

  private val arrayPrefix: P[Int] = P("*" ~/ "-".!.? ~/ digit.rep(1).!).map {
    case (minus, n) =>
      minus.map(_ => -1).getOrElse(1) * n.toInt
  }

  private def array[A <: Expr](elementExpr: P[A]): P[ArrayExpr[A]] =
    P(arrayPrefixWithCrLf ~/ elementExpr.rep(sep = crlf)).map {
      case (ArraySizeExpr(-1), _) =>
        ArrayExpr(Seq.empty)
      case (size, values) =>
        require(size.value == values.size)
        ArrayExpr(values)
    }

  private val stringOptArrayElement: P[StringOptExpr] = P(length ~/ crlf).flatMap { l =>
    if (l.value == -1)
      End.map(_ => StringOptExpr(None))
    else
      AnyChars(l.value).!.map(s => StringOptExpr(Some(s)))
  }

  private val integerArrayElement: P[NumberExpr] = P(number)

  private def bulkStringRest(l: Int): P[StringOptExpr] = {
    if (l == -1) {
      P(End).!.map(_ => StringOptExpr(None))
    } else {
      P(AnyChars(l).! ~/ crlf).map(v => StringOptExpr(Some(v)))
    }
  }

  val errorReply: P[ErrorExpr]                         = P("-" ~/ (!crlf ~/ AnyChar).rep(1).! ~ crlf).map(ErrorExpr)
  val simpleStringReply: P[SimpleExpr]                 = P(simple ~/ crlf)
  val bulkStringReply: P[StringOptExpr]                = P((length ~/ crlf).flatMap(l => bulkStringRest(l.value)))
  val integerReply: P[NumberExpr]                      = P(number ~/ crlf)
  val arrayPrefixWithCrLf: P[ArraySizeExpr]            = P(arrayPrefix ~/ crlf).map(ArraySizeExpr)
  val stringOptArrayReply: P[ArrayExpr[StringOptExpr]] = P(array(stringOptArrayElement) ~/ crlf.?)
  val integerArrayReply: P[ArrayExpr[NumberExpr]]      = P(array(integerArrayElement) ~/ crlf.?)
  val stringArrayReply: P[Expr] = P(stringOptArrayReply).map { v =>
    ArrayExpr(v.values.map(_.toStringExpr))
  }

}
