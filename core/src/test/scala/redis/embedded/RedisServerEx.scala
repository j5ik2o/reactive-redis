package redis.embedded

import java.io.File
import java.net.InetSocketAddress
import scala.collection.JavaConverters._

object RedisServerEx {
  def toString(slaveOf: Option[InetSocketAddress] = None) =
    if (slaveOf.isEmpty) List.empty
    else
      List(
        "--slaveof",
        slaveOf.get.getHostName,
        slaveOf.get.getPort.toString
      )
}
class RedisServerEx(args: java.util.List[String], port: Int) extends RedisServer(args, port) {

//  def this(port: Int) = {
//    this(
//      java.util.Arrays.asList(RedisExecProvider.defaultProvider.get.getAbsolutePath, "--port", port.toString),
//      port
//    )
//  }
//
//  def this(executable: File, port: Int) = {
//    this(List(executable.getAbsolutePath, "--port", port.toString).asJava, port)
//  }

  def this(redisExecProvider: RedisExecProvider, port: Int, slaveOf: Option[InetSocketAddress] = None) = {
    this(
      (List(redisExecProvider.get().getAbsolutePath, "--port", port.toString) ++ RedisServerEx
        .toString(slaveOf)).asJava,
      port
    )
  }

  override def redisReadyPattern(): String = ".*Ready to accept connections"
}
