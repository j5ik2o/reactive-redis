package com.github.j5ik2o.reactive.redis.cats.free

import cats.free.Free
import cats.implicits._
import com.github.j5ik2o.reactive.redis.RedisFutureClient

import scala.concurrent.{ ExecutionContext, Future }

class RedisCommandExecutor(redisFutureClient: RedisFutureClient)(implicit ec: ExecutionContext) {

  private val interpreter = new StringsInterpreter(redisFutureClient)

  def run[A](program: Free[StringsDSL, A]): Future[A] = program.foldMap(interpreter)

}
