package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Tcp.OutgoingConnection
import akka.stream.scaladsl.{ Flow, GraphDSL, Keep, Sink, Source, Tcp, Unzip, Zip }
import akka.stream.{ FlowShape, Materializer }
import akka.util.ByteString

import scala.concurrent.Future
import scala.util.parsing.input.CharSequenceReader

object RedisAPIExecutor {
  type CON = Flow[ByteString, ByteString, Future[OutgoingConnection]]

  def apply(connection: RedisAPIExecutor.CON) = new RedisAPIExecutor(connection)

  def apply(address: InetSocketAddress)(implicit system: ActorSystem) = new RedisAPIExecutor(Tcp().outgoingConnection(address))
}

class RedisAPIExecutor(connection: RedisAPIExecutor.CON) {

  import BaseProtocol._

  private lazy val toByteStringFlow: Flow[String, ByteString, NotUsed] = Flow[String].map { s => ByteString(s) }

  private lazy val parseFlow: Flow[ByteString, String, NotUsed] = Flow[ByteString]
    //.via(Framing.delimiter(ByteString("\r\n"), maximumFrameLength = Int.MaxValue, allowTruncation = true))
    .map(_.utf8String)
    .log("response")
  //.fold(Seq.empty[String])((acc, in) => acc :+ in)

  private lazy val internalRequestFlow = Flow[String].via(toByteStringFlow)
    .fold(ByteString.empty)((acc, in) => acc ++ in)
    .via(connection)
    .via(parseFlow)

  private lazy val requestFlow: Flow[CommandRequest, (String, Seq[CommandRequest]), NotUsed] = Flow.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    // String, Seq[CR]
    val flowShape = builder.add(Flow[CommandRequest].fold(("", Seq.empty[CommandRequest])) { (acc, in) =>
      (acc._1 ++ (in.encodeAsString + "\r\n"), acc._2 :+ in)
    })
    val unzip = builder.add(Unzip[String, Seq[CommandRequest]]())
    val zip = builder.add(Zip[String, Seq[CommandRequest]]())
    flowShape ~> unzip.in
    unzip.out0 ~> internalRequestFlow ~> zip.in0
    unzip.out1 ~> zip.in1
    FlowShape(flowShape.in, zip.out)
  })

  private def resultFlow[A <: CommandRequest] = Flow[(String, Seq[CommandRequest])].map {
    case (result, requests) =>
      requests.foldLeft((Seq.empty[A#ResponseType], new CharSequenceReader(result))) { (acc, request) =>
        val p = request.parser
        val (parseResult, next) = p.parseResponse(acc._2)
        (acc._1 :+ parseResult.asInstanceOf[A#ResponseType], next.asInstanceOf[CharSequenceReader])
      }._1
  }

  def executeFlow[A <: CommandRequest](implicit mat: Materializer): Flow[A, Seq[A#ResponseType], NotUsed] = {
    Flow[A]
      .log("request", (cmd) => cmd.encodeAsString)
      .via(requestFlow)
      .via(resultFlow[A])
  }

  def execute[A <: CommandRequest](source: Source[A, NotUsed])(implicit mat: Materializer): Future[Seq[A#ResponseType]] = {
    source
      .via(executeFlow[A])
      .toMat(Sink.head)(Keep.right)
      .run()
  }

}

