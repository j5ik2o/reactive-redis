package com.github.j5ik2o.reactive.redis

import java.net.InetSocketAddress

import akka.io.{ IO, Tcp }
import akka.stream.actor.{ ActorPublisher, ActorSubscriber }
import akka.stream.scaladsl.Source
//
//class CommandSubscriber(remote: InetSocketAddress) extends ActorSubscriber {
//  import akka.io.Tcp._
//  import context.system
//  IO(Tcp) ! Connect(remote)
//  Source.ac
//  override def receive: Receive = {
//    case c @ Connected(remote, local) =>
//      val connection = sender()
//      connection ! Register(self)
//
//  }
//}
