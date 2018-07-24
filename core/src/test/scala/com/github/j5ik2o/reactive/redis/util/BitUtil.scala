package com.github.j5ik2o.reactive.redis.util

import akka.util.ByteString
import com.github.j5ik2o.reactive.redis.command.strings.StartAndEnd

object BitUtil {

  def getBitCount(s: String, startAndEnd: Option[StartAndEnd] = None): Int = {
    val bitStr =
      if (startAndEnd.isEmpty)
        ByteString(s).map(toBinaryString).mkString
      else
        ByteString(s).slice(startAndEnd.get.start, startAndEnd.get.end + 1).map(toBinaryString).mkString

    bitStr
      .count(_ == '1')
  }

  def toBinaryString(b: Byte): String = {
    val i  = new Array[Int](8)
    val bs = new StringBuffer
    i(0) = (b & 0x80) >>> 7
    i(1) = (b & 0x40) >>> 6
    i(2) = (b & 0x20) >>> 5
    i(3) = (b & 0x10) >>> 4
    i(4) = (b & 0x08) >>> 3
    i(5) = (b & 0x04) >>> 2
    i(6) = (b & 0x02) >>> 1
    i(7) = (b & 0x01) >>> 0
    var j = 0
    while ({
      j < 8
    }) {
      bs.append(i(j))

      {
        j += 1; j - 1
      }
    }
    bs.toString
  }

}
