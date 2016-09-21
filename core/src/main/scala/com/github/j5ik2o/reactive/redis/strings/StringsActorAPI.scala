package com.github.j5ik2o.reactive.redis.strings

import akka.actor.Actor
import com.github.j5ik2o.reactive.redis.BaseActorAPI

trait StringsActorAPI
    extends BaseActorAPI with StringCommandRequests {
  this: Actor =>

  val handleStrings: Receive = {
    PartialFunction.empty[Any, Unit]
    //    case SetRequest(key, value) =>
    //      run(set(key, value)).pipeTo(sender())
    //    case GetRequest(key) =>
    //      run(get(key)).pipeTo(sender())
    //    case GetSetRequest(key, value) =>
    //      run(getSet(key, value)).pipeTo(sender())
  }

}
