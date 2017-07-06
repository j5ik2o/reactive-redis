package com.github.j5ik2o.reactive.redis.cats.free

import cats.~>
import com.github.j5ik2o.reactive.redis.RedisFutureClient

import scala.concurrent.{ ExecutionContext, Future }

class StringsInterpreter(redisFutureClient: RedisFutureClient)(implicit ec: ExecutionContext)
    extends (StringsDSL ~> Future) {

  override def apply[A](fa: StringsDSL[A]): Future[A] = fa match {
    case StringsCommand.Append(key, value) =>
      redisFutureClient.append(key, value).asInstanceOf[Future[A]]
    case StringsCommand.Get(key) =>
      redisFutureClient.get(key).asInstanceOf[Future[A]]
  }

}
