package com.github.j5ik2o.reactive.redis

import scala.util.matching.Regex
import scala.util.parsing.combinator.Parsers
import scala.util.parsing.input.{ Position, Reader }
import java.lang.Double.longBitsToDouble
import java.lang.Float.intBitsToFloat

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

  case class BytesExpr(value: Array[Byte]) extends Expr with HasSize {
    override val size: Int = value.length
  }

  case class StringOptExpr(value: Option[String]) extends Expr with HasSize {
    override val size: Int = value.fold(0)(_.length)
  }

  case class ArraySizeExpr(value: Int) extends Expr

  case class ArrayExpr[A <: Expr](values: Seq[A] = Seq.empty) extends Expr

}

trait ParsersUtil { this: Parsers =>
  lazy val anyElem: Parser[Elem] = elem("anyElem", _ => true)

  def elemExcept(xs: Elem*): Parser[Elem] = elem("elemExcept", x => !(xs contains x))

  def elemOf(xs: Elem*): Parser[Elem] = elem("elemOf", xs contains _)

  def take(n: Int): Parser[Seq[Elem]] = repN(n, anyElem)

  def takeUntil(cond: Parser[_]): Parser[Seq[Elem]] = takeUntil(cond, anyElem)

  def takeUntil(cond: Parser[_], p: Parser[Elem]): Parser[Seq[Elem]] = rep(not(cond) ~> p)

  def takeWhile(p: Parser[Elem]): Parser[Seq[Elem]] = rep(p)
}

case class ByteOffsetPosition(offset: Int) extends Position {
  final val line = 1

  def column = offset + 1

  def lineContents: String = ""
}

import scala.util.parsing.input.CharArrayReader.EofCh

class ByteReader(val bytes: Array[Byte], override val offset: Int) extends Reader[Byte] {
  def this(reader: Reader[_]) = this(reader.source.toString.getBytes, 0)

  def this(bytes: Seq[Byte]) = this(bytes.toArray, 0)

  def this(str: String) = this(str.getBytes, 0)

  override def source: ArrayCharSequence = bytes map (_.toChar)

  def first: Byte = if (offset < bytes.length) bytes(offset) else EofCh.toByte

  def rest: ByteReader = if (offset < bytes.length) new ByteReader(bytes, offset + 1) else this

  def pos: Position = ByteOffsetPosition(offset)

  def atEnd: Boolean = offset >= bytes.length

  def byteAt(n: Int): Byte = bytes(n)

  def length: Int = bytes.length - offset

  override def drop(n: Int): ByteReader = new ByteReader(bytes, offset + n)

  def take(n: Int): Seq[Byte] = bytes.slice(offset, offset + n)

  override def toString: String = "ByteReader(%d / %d)".format(offset, bytes.length)
}

class SubSequence(s: CharSequence, start: Int, val length: Int) extends CharSequence {
  def this(s: CharSequence, start: Int) = this(s, start, s.length - start)

  def charAt(i: Int): Char =
    if (i >= 0 && i < length) s.charAt(start + i)
    else throw new IndexOutOfBoundsException(s"index: $i, length: $length")

  def subSequence(start: Int, end: Int): SubSequence = {
    if (start < 0 || end < 0 || end > length || start > end)
      throw new IndexOutOfBoundsException(s"start: $start, end: $end, length: $length")

    new SubSequence(s, this.start + start, end - start)
  }

  override def toString: String = s.subSequence(start, start + length).toString
}

trait BinaryParsers extends Parsers with ParsersUtil {
  type Elem = Byte

  protected implicit def readerToByteReader(x: Input): ByteReader = x match {
    case br: ByteReader => br
    case _              => new ByteReader(x)
  }

  protected implicit def toByte(ch: Char): Byte = ch.toByte

  def toInt(bytes: Seq[Byte]): Int = bytes.foldLeft(0)((x, b) => (x << 8) + (b & 0xFF))

  def toLong(bytes: Seq[Byte]): Long = bytes.foldLeft(0L)((x, b) => (x << 8) + (b & 0xFF))

  lazy val byte: Parser[Byte]  = anyElem
  lazy val u1: Parser[Int]     = byte ^^ (_ & 0xFF)
  lazy val u2: Parser[Int]     = bytes(2) ^^ toInt
  lazy val u4: Parser[Int]     = bytes(4) ^^ toInt
  lazy val u4f: Parser[Float]  = u4 ^^ intBitsToFloat
  lazy val u8: Parser[Long]    = bytes(8) ^^ toLong
  lazy val u8d: Parser[Double] = u8 ^^ longBitsToDouble

  def bytes(n: Int): Parser[Seq[Byte]] = Parser { in =>
    if (n <= in.length) Success(in take n, in drop n)
    else Failure("Requested %d bytes but only %d remain".format(n, in.length), in)
  }

//  override def phrase[T](p: Parser[T]): Parser[T] =
//    super.phrase(p <~ "".r)

  def parseFromInput[T](p: Parser[T], in: Input): ParseResult[T] = p(in)

  def parseFromString[T](p: Parser[T], in: String): ParseResult[T] = p(new ByteReader(in))

  def parseFromByteReader[T](p: Parser[T], in: Reader[Byte]): ParseResult[T] = p(in)

  def parseFromCharReader[T](p: Parser[T], in: Reader[Char]): ParseResult[T] = p(new ByteReader(in))

  protected val whiteSpace = """\s+""".r

  def skipWhitespace = whiteSpace.toString.length > 0

  /** Method called to handle whitespace before parsers.
    *
    * It checks `skipWhitespace` and, if true, skips anything
    * matching `whiteSpace` starting from the current offset.
    *
    * @param source The input being parsed.
    * @param offset The offset into `source` from which to match.
    * @return The offset to be used for the next parser.
    */
  protected def handleWhiteSpace(source: java.lang.CharSequence, offset: Int): Int =
    if (skipWhitespace)
      whiteSpace findPrefixMatchOf new SubSequence(source, offset) match {
        case Some(matched) => offset + matched.end
        case None          => offset
      } else
      offset

  /** A parser that matches a literal string */
  implicit def literal(s: String): Parser[String] = Parser[String] { in =>
    val source = in.source
    val offset = in.offset
    val start  = handleWhiteSpace(source, offset)
    var i      = 0
    var j      = start
    while (i < s.length && j < source.length && s.charAt(i) == source.charAt(j)) {
      i += 1
      j += 1
    }
    if (i == s.length)
      Success(source.subSequence(start, j).toString, in.drop(j - offset))
    else {
      val found = if (start == source.length()) "end of source" else "`" + source.charAt(start) + "'"
      Failure("`" + s + "' expected but " + found + " found", in.drop(start - offset))
    }
  }

  /** A parser that matches a regex string */
  implicit def regex(r: Regex): Parser[String] = Parser[String] { in =>
    val source = in.source
    val offset = in.offset
    val start  = handleWhiteSpace(source, offset)
    r findPrefixMatchOf new SubSequence(source, start) match {
      case Some(matched) =>
        Success(source.subSequence(start, start + matched.end).toString, in.drop(start + matched.end - offset))
      case None =>
        val found = if (start == source.length()) "end of source" else "`" + source.charAt(start) + "'"
        Failure("string matching regex `" + r + "' expected but " + found + " found", in.drop(start - offset))
    }
  }

}

trait CommandResponseParserSupport extends BinaryParsers {

  import CommandResponseParser._

  override def skipWhitespace: Boolean = false

  private lazy val crlf: Parser[String] = """\r\n""".r

  private lazy val error: Parser[ErrorExpr] = elem('-') ~> """[a-zA-Z0-9. ]+""".r ^^ { msg =>
    ErrorExpr(msg)
  }

  private lazy val length: Parser[LengthExpr] = elem('$') ~> """[0-9-]+""".r ^^ { n =>
    LengthExpr(n.toInt)
  }

  private lazy val simple: Parser[SimpleExpr] = elem('+') ~> """[a-zA-Z0-9. ]+""".r ^^ { msg =>
    SimpleExpr(msg)
  }

  private lazy val number: Parser[NumberExpr] = elem(':') ~> """[-0-9]+""".r ^^ { n =>
    NumberExpr(n.toInt)
  }

  private lazy val stringValue: Parser[String] = rep(not(crlf) ~> anyElem) ^^ { array =>
    new String(array.toArray, "UTF-8")
  }

  private lazy val string: Parser[StringExpr] = stringValue ^^ { s =>
    StringExpr(s)
  }

  private lazy val arrayPrefix: Parser[Int] = elem('*') ~> """[0-9]+""".r ^^ (_.toInt)

  private lazy val errorWithCrLf: Parser[Expr] = error <~ crlf

  private lazy val simpleWithCrLf: Parser[Expr] = simple <~ crlf

  private lazy val integerWithCrLf: Parser[NumberExpr] = number <~ crlf

  private lazy val arrayPrefixWithCrLf: Parser[ArraySizeExpr] = arrayPrefix <~ crlf ^^ { n =>
    ArraySizeExpr(n)
  }

  def array[A <: Expr](elementExpr: Parser[A]): Parser[ArrayExpr[A]] =
    arrayPrefixWithCrLf ~ repsep(
      elementExpr,
      crlf
    ) ^^ {
      case size ~ values =>
        require(size.value == values.size)
        ArrayExpr(values)
    }

  private lazy val stringOptArrayElement: Parser[StringOptExpr] = length ~ crlf ~ opt(stringValue) ^^ {
    case size ~ _ ~ _ if size.value == -1 =>
      StringOptExpr(None)
    case size ~ _ ~ value =>
      // require(size.value == -1 || size.value == value.size)
      StringOptExpr(value)
  }

  private lazy val integerArrayElement: Parser[NumberExpr] = opt(length <~ crlf) ~ number ^^ {
    case size ~ value =>
      // require(size.map(_.value).fold(true)(_ == value.size))
      value
  }

  private lazy val stringOptArrayWithCrLf: Parser[ArrayExpr[StringOptExpr]] = array(stringOptArrayElement)

  private lazy val integerArrayWithCrLf: Parser[ArrayExpr[NumberExpr]] = array(integerArrayElement)

  private lazy val bulkStringWithCrLf: Parser[StringOptExpr] = length ~ crlf ~ opt(stringValue <~ crlf) ^^ {
    case l ~ _ ~ s =>
      StringOptExpr(s)
  }

  private lazy val bulkBytesWithCrLf: Parser[BytesExpr] = length <~ crlf >> { _ =>
    rep(not(crlf) ~> anyElem) ^^ { byteSeq =>
      BytesExpr(byteSeq.toArray)
    }
  }

  lazy val simpleStringReply: Parser[Expr] = simpleWithCrLf | errorWithCrLf

  lazy val integerReply: Parser[Expr] = integerWithCrLf | errorWithCrLf

  lazy val arrayPrefixWithCrLfOrErrorWithCrLf: Parser[Expr] = arrayPrefixWithCrLf | errorWithCrLf

  lazy val integerArrayReply: Parser[Expr] = integerArrayWithCrLf | errorWithCrLf

  lazy val stringOptArrayReply: Parser[Expr] = stringOptArrayWithCrLf | errorWithCrLf

  lazy val bulkStringReply: Parser[Expr] = bulkStringWithCrLf | errorWithCrLf

  lazy val bulkBytesReply: Parser[Expr] = bulkBytesWithCrLf | errorWithCrLf

  protected val responseParser: Parser[Expr]

  def parseResponseToExprWithInput(in: Reader[Byte]): (Expr, Input) = parseFromByteReader(responseParser, in) match {
    case Success(result, next) => (result, next)
    case Failure(msg, _)       => throw new Exception(msg)
    case Error(msg, _)         => throw new Exception(msg)
  }

}
