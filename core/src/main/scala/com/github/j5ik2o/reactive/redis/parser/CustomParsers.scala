package com.github.j5ik2o.reactive.redis.parser

import com.github.j5ik2o.reactive.redis.parser.model._
import com.github.j5ik2o.reactive.redis.parser.util.Parsers

@SuppressWarnings(
  Array("org.wartremover.warts.Product",
        "org.wartremover.warts.Serializable",
        "org.wartremover.warts.Equals",
        "org.wartremover.warts.EitherProjectionPartial")
)
object CustomParsers {

  def getParer[Parser[+ _]](P: Parsers[Parser]): Parser[Expr] = {
    import P.{ string => _, _ }
    implicit def tok(s: String): Parser[String] = token(P.string(s))

    lazy val error: Parser[ErrorExpr]   = surround("-", "\r\n")(".*".r).map(ErrorExpr)
    lazy val simple: Parser[SimpleExpr] = surround("+", "\r\n")(".*".r).map(SimpleExpr)

    lazy val length: Parser[LengthExpr] = surround("$", "\r\n")(opt("-") ** digits.many1) map {
      case (m, n) =>
        LengthExpr(m.map(_ => -1).getOrElse(1) * n.mkString.toInt)
    }

    def bulkStringRest(l: Int): Parser[StringOptExpr] = {
      if (l == -1)
        eof.map(_ => StringOptExpr(None))
      else
        ".*".r ** "\r\n" map { case (value, _) => StringOptExpr(Some(value)) }
    }
    lazy val bulkStringReply = length.flatMap { l =>
      bulkStringRest(l.value)
    }

    root(error | simple | bulkStringReply)
  }

}
