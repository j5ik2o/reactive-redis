package com.github.j5ik2o.reactive.redis

import java.text.ParseException

import akka.NotUsed
import akka.stream.scaladsl.{ Flow, Framing, Keep, Sink }
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.util.ByteString

import scala.concurrent.Future

trait BaseStreamAPI {

  val digitsRegex = """:(\d+)$""".r
  val stringRegex = """\+(\w+)$""".r
  val errorRegex = """\-(\w+)$""".r
  val dollorRegex = """\$([0-9-]+)$""".r
  val listSizeRegex = """\*(\d+)$""".r

  def parseException = new ParseException("protocol parse error", 0)

  protected val connection: Flow[ByteString, ByteString, Future[OutgoingConnection]]

  protected val toByteString: Flow[String, ByteString, NotUsed] = Flow[String].map { s => ByteString(s.concat("\r\n")) }

  protected val sink: Sink[ByteString, Future[Seq[String]]] = Flow[ByteString]
    .via(Framing.delimiter(ByteString("\r\n"), maximumFrameLength = Int.MaxValue, allowTruncation = true))
    .map(_.utf8String)
    .log("response")
    .toMat(Sink.fold(Seq.empty[String])((acc, in) => acc :+ in))(Keep.right)


}
