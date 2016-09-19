package com.github.j5ik2o.reactive.redis

object ResponseRegexs {
  val integerRegex = """:(\d+)$""".r
  val simpleStringRegex = """\+(\w+)$""".r
  val messageRegex = """\+(.*)$""".r
  val errorRegex = """\-(.*)$""".r
  val dollorRegex = """\$([0-9-]+)$""".r
  val arrayRegex = """\*(\d+)$""".r
}
