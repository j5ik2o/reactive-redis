package com.github.j5ik2o.reactive.redis

import org.scalacheck.Gen

trait ScalaCheckSupport {

  val keyNumValueGen: Gen[(String, Int)] = for {
    key   <- Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(_.nonEmpty)
    value <- Gen.choose(Int.MinValue, Int.MaxValue)
  } yield (key, value)

  val keyStrValueGen: Gen[(String, String)] = for {
    key   <- Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(_.nonEmpty)
    value <- Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(_.nonEmpty)
  } yield (key, value)

  def keyStrValuesGen(valuesMinSize: Int = 1): Gen[(String, List[String])] =
    for {
      key <- Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(_.nonEmpty)
      values <- Gen
        .listOfN(valuesMinSize, Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(_.nonEmpty))
        .suchThat(_.nonEmpty)
    } yield (key, values)

  val keyFieldStrValueGen: Gen[(String, String, String)] = for {
    key   <- Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(_.nonEmpty)
    field <- Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(_.nonEmpty)
    value <- Gen.listOf(Gen.alphaNumChar).map(_.mkString).suchThat(_.nonEmpty)
  } yield (key, field, value)

}
