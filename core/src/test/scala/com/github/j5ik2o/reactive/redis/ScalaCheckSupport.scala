package com.github.j5ik2o.reactive.redis

import org.scalacheck.Gen

trait ScalaCheckSupport {

  val keyValueGen: Gen[(String, String)] = for {
    key   <- Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(_.nonEmpty)
    value <- Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(_.nonEmpty)
  } yield (key, value)

  val keyValuesGen: Gen[(String, List[String])] = for {
    key    <- Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(_.nonEmpty)
    values <- Gen.listOf(Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(_.nonEmpty)).suchThat(_.nonEmpty)
  } yield (key, values)

}
