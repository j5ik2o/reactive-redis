package com.github.j5ik2o.reactive.redis

import akka.actor.Actor
import akka.stream.ActorMaterializer

trait BaseActorAPI {
  this: Actor =>

  implicit val materializer = ActorMaterializer()

  implicit val system = context.system

  import context.dispatcher

}
