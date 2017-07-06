package com.github.j5ik2o.reactive.redis.cats.free

import cats.free._
import cats.~>
import com.github.j5ik2o.reactive.redis.RedisFutureClient

import scala.concurrent.{ ExecutionContext, Future }

trait StringsDSL[A] extends RedisDSL[A]

trait StringsFreeFeature {

  // --- APPEND

  case class Append(key: String, value: String) extends StringsDSL[Option[Int]]

  // --- BITCOUNT

  // --- GET

  case class Get(key: String) extends StringsDSL[Option[String]]

  def append(key: String, value: String): Free[StringsDSL, Option[Int]] = Free.liftF(Append(key, value))

  def get(key: String): Free[StringsDSL, Option[String]] = Free.liftF(Get(key))

  protected class StringsInterpreter(redisFutureClient: RedisFutureClient)(implicit ec: ExecutionContext)
      extends (StringsDSL ~> Future) {

    override def apply[A](fa: StringsDSL[A]): Future[A] = fa match {
      case Append(key, value) =>
        redisFutureClient.append(key, value).asInstanceOf[Future[A]]
      case Get(key) =>
        redisFutureClient.get(key).asInstanceOf[Future[A]]
    }

  }
}
