package com.github.j5ik2o.reactive.redis

import scala.util.parsing.combinator.RegexParsers
import scala.util.parsing.input.Reader

object CommandResponseParser {

  sealed trait Expr

  trait HasSize extends Expr {
    val size: Int
  }

  case class ErrorExpr(msg: String) extends Expr

  case class LengthExpr(value: Int) extends Expr

  case class SimpleExpr(msg: String) extends Expr with HasSize {
    override val size: Int = msg.length
  }

  case class NumberExpr(value: Int) extends Expr with HasSize {
    override val size: Int = value.toString.length
  }

  case class StringExpr(value: String) extends Expr with HasSize {
    override val size: Int = value.length
  }

  case class ArrayExpr[A <: Expr](values: Seq[A] = Seq.empty) extends Expr

}

abstract class CommandResponseParser[RT] extends RegexParsers {

  import CommandResponseParser._

  override def skipWhitespace: Boolean = false

  lazy val ERROR: Parser[ErrorExpr] = elem('-') ~> """[a-zA-Z0-9. ]+""".r ^^ { msg => ErrorExpr(msg) }

  lazy val LENGTH: Parser[LengthExpr] = elem('$') ~> """[0-9-]+""".r ^^ { n => LengthExpr(n.toInt) }

  lazy val SIMPLE: Parser[SimpleExpr] = elem('+') ~> """[a-zA-Z0-9. ]+""".r ^^ { msg => SimpleExpr(msg) }

  lazy val NUMBER: Parser[NumberExpr] = elem(':') ~> """[0-9]+""".r ^^ { n => NumberExpr(n.toInt) }

  lazy val VALUE: Parser[StringExpr] = """.*[^\r\n]""".r ^^ { s => StringExpr(s) }

  lazy val NUMBER_OR_VALUE: Parser[Expr with HasSize] = NUMBER | VALUE

  lazy val ARRAY_PREFIX: Parser[Int] = elem('*') ~> """[0-9]+""".r ^^ { n =>
    n.toInt
  }

  lazy val CRLF = """\r\n""".r

  lazy val STRING_ARRAY_VALUE: Parser[StringExpr] = LENGTH ~ CRLF ~ VALUE ^^ { case size ~ _ ~ value =>
    require(size.value == value.size)
    value
  }

  lazy val NUMBER_ARRAY_VALUE: Parser[NumberExpr] = LENGTH ~ CRLF ~ NUMBER ^^ { case size ~ _ ~ value =>
    require(size.value == value.size)
    value
  }

  lazy val errorWithCrLf: Parser[Expr] = ERROR <~ CRLF

  lazy val simpleWithCrLf: Parser[Expr] = SIMPLE <~ CRLF

  lazy val numberWithCrLf = NUMBER <~ CRLF

  lazy val simpleWithCrLfOrErrorWithCrLf: Parser[Expr] = simpleWithCrLf | errorWithCrLf

  lazy val numberWithCrLfOrErrorWithCrLf: Parser[Expr] = numberWithCrLf | errorWithCrLf

  lazy val arrayPrefixWithCrLf = ARRAY_PREFIX <~ CRLF

  lazy val stringArrayWithCrLf: Parser[ArrayExpr[StringExpr]] = arrayPrefixWithCrLf ~ repsep(STRING_ARRAY_VALUE, CRLF) ^^ { case size ~ values =>
    require(size == values.size)
    ArrayExpr(values)
  }

  lazy val numberArrayWithCrLf: Parser[ArrayExpr[NumberExpr]] = arrayPrefixWithCrLf ~ repsep(NUMBER_ARRAY_VALUE, CRLF) ^^ { case size ~ values =>
    require(size == values.size)
    ArrayExpr(values)
  }

  lazy val stringArrayWithCrLfOrErrorWithCrLf: Parser[Expr] = stringArrayWithCrLf | errorWithCrLf

  lazy val bulkString = STRING_ARRAY_VALUE

  lazy val bulkStringWithCrLf = bulkString <~ CRLF

  protected val responseParser: Parser[RT]

  def parseResponse(in: Reader[Char]) = parse(responseParser, in) match {
    case Success(result, next) => (result, next)
    case Failure(msg, _)    => throw new Exception(msg)
    case Error(msg, _)      => throw new Exception(msg)
  }

}


trait CommandRequest extends RegexParsers {

  import ResponseRegexs._

  def encodeAsString: String

  override def toString: String = encodeAsString

  type ResultType

  type ResponseType <: CommandResponse

  def responseAsSucceeded(arguments: ResultType): ResponseType

  def responseAsFailed(ex: Exception): ResponseType

  val parser: CommandResponseParser[ResponseType]

}
