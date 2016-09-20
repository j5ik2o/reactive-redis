package com.github.j5ik2o.reactive.redis.connection

import akka.actor.Actor
import com.github.j5ik2o.reactive.redis.BaseActorAPI
import akka.pattern.pipe
import akka.pattern.{ ask, pipe }

trait ConnectionActorAPI extends BaseActorAPI with ConnectionCommandRequests {
  this: Actor =>

  import ConnectionProtocol._

  import context.dispatcher

  val handleConnection: Receive = {
    //    case QuitRequest =>
    //      quit.map { v =>
    //        QuitSucceeded
    //      }.pipeTo(sender())
    //    case SelectRequest(index) =>
    //      select(index).map { _ =>
    //        SelectSucceeded
    //      }.recover { case ex: Exception =>
    //        SelectFailure(ex)
    //      }.pipeTo(sender())
    case _ =>
  }

}
