package com.github.j5ik2o.reactive.redis

import java.text.ParseException

import akka.NotUsed
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{ Flow, Framing, GraphDSL, Keep, Sink, Source, Unzip, Zip }
import akka.stream.{ FlowShape, Materializer }
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.parsing.input.CharSequenceReader

trait BaseStreamAPI {

  protected def parseException(msg: Option[String] = None) = new ParseException(msg.orNull, 0)

  protected val connection: Flow[ByteString, ByteString, Future[OutgoingConnection]]

  protected lazy val toByteStringFlow: Flow[String, ByteString, NotUsed] = Flow[String].map { s => ByteString(s) }

  protected lazy val parseFlow: Flow[ByteString, String, NotUsed] = Flow[ByteString]
    //.via(Framing.delimiter(ByteString("\r\n"), maximumFrameLength = Int.MaxValue, allowTruncation = true))
    .map(_.utf8String)
    .log("response")

    //.fold(Seq.empty[String])((acc, in) => acc :+ in)

  protected def requestFlow = Flow[String].via(toByteStringFlow).fold(ByteString.empty)((acc, in) => acc ++ in).log("cmd").via(connection).via(parseFlow)

  def graph[A <: CommandResponse] = GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    // String, Seq[CR]
    val flowShape = builder.add(Flow[CommandRequest].fold(("", Seq.empty[CommandRequest])) { (acc, in) =>
      (acc._1 ++ (in.encodeAsString + "\r\n"), acc._2 :+ in)
    })
    val unzip = builder.add(Unzip[String, Seq[CommandRequest]]())
    val zip = builder.add(Zip[String, Seq[CommandRequest]]())
    flowShape ~> unzip.in
    unzip.out0 ~> requestFlow ~> zip.in0
    unzip.out1 ~> zip.in1
    FlowShape(flowShape.in, zip.out)
  }

  def mainFlow[A <: CommandResponse]: Flow[CommandRequest, (String, Seq[CommandRequest]), NotUsed] = Flow.fromGraph(graph[A])

  def resultFlow[A <: CommandResponse] =  Flow[(String, Seq[CommandRequest])].map { case (result, requests) =>
    val reader = new CharSequenceReader(result)

    requests.foldLeft((Seq.empty[CommandResponse], reader)){ (acc, request) =>
      val p = request.parser
      val (parseResult, next) = p.parseResponse(acc._2)
      (acc._1 :+ parseResult, next.asInstanceOf[CharSequenceReader])
    }._1
  }

  def run[A <: CommandRequest](source: Source[A, NotUsed])(implicit mat: Materializer) = {
    source
      .log("request", (cmd) => cmd.encodeAsString)
      .via(mainFlow)
      .via(resultFlow)
      .toMat(Sink.head)(Keep.right)
      .run()
  }

  protected val sink: Sink[ByteString, Future[Seq[String]]] = Flow[ByteString]
    .via(Framing.delimiter(ByteString("\r\n"), maximumFrameLength = Int.MaxValue, allowTruncation = true))
    .map(_.utf8String)
    .log("response")
    .toMat(Sink.fold(Seq.empty[String])((acc, in) => acc :+ in))(Keep.right)


}
