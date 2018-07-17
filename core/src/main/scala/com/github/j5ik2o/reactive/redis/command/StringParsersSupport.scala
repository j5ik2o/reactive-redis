package com.github.j5ik2o.reactive.redis.command

import scodec.bits.ByteVector

trait StringParsersSupport { this: CommandRequestBase =>
  override type Elem = Char
  override type Repr = String
  override protected def convertToParseSource(s: ByteVector): String = s.decodeUtf8.right.get
}
