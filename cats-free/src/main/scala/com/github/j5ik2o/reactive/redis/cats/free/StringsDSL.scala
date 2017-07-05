package com.github.j5ik2o.reactive.redis.cats.free

import cats.free._

trait StringsDSL[A] extends RedisDSL[A]

trait StringsCommand {

  // --- APPEND

  case class Append(key: String, value: String) extends StringsDSL[Option[Int]]

  def append(key: String, value: String): Free[StringsDSL, Option[Int]] = Free.liftF(Append(key, value))

  // --- BITCOUNT

  // --- GET

  case class Get(key: String) extends StringsDSL[Option[String]]

  def get(key: String): Free[StringsDSL, Option[String]] = Free.liftF(Get(key))

}

object StringsCommand extends StringsCommand
