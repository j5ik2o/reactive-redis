package com.github.j5ik2o.reactive.redis.scalaz.free

import com.github.j5ik2o.reactive.redis.RedisFutureClient

import scala.concurrent.{ ExecutionContext, Future }
import scalaz.Free
import scalaz.std.scalaFuture._

class RedisFreeClient(redisFutureClient: RedisFutureClient)(implicit ec: ExecutionContext) extends StringsFreeFeature {

  private val interpreter = new StringsInterpreter(redisFutureClient)

  def run[A](program: Free[StringsDSL, A]): Future[A] = program.foldMap(interpreter)

}
