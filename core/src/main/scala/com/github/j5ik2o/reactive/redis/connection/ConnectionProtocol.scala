package com.github.j5ik2o.reactive.redis.connection

object ConnectionProtocol {

  // --- AUTH

  // --- ECHO

  // --- PING

  // --- QUIT
  case object QuitRequest

  case object QuitSucceeded

  // --- SELECT
  case class SelectRequest(index: Int)

  case object SelectSucceeded

  case class SelectFailure(ex: Exception)

}
