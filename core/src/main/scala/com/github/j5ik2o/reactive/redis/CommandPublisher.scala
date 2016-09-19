package com.github.j5ik2o.reactive.redis

import akka.stream.actor.ActorPublisher

import scala.annotation.tailrec

//class CommandPublisher extends ActorPublisher[CommandRequest] {
//
//  import akka.stream.actor.ActorPublisherMessage._
//
//  val MaxBufferSize = 100
//  var buf = Vector.empty[CommandRequest]
//
//  override def receive: Receive = {
//    case msg: CommandRequest =>
//      if (buf.isEmpty && totalDemand > 0)
//        onNext(msg)
//      else {
//        buf :+= msg
//        deliverBuf()
//      }
//
//  }
//
//  @tailrec final def deliverBuf(): Unit =
//    if (totalDemand > 0) {
//      if (totalDemand <= Int.MaxValue) {
//        val (use, keep) = buf.splitAt(totalDemand.toInt)
//        buf = keep
//        use foreach onNext
//      } else {
//        val (use, keep) = buf.splitAt(Int.MaxValue)
//        buf = keep
//        use foreach onNext
//        deliverBuf()
//      }
//    }
//}
