package com.github.j5ik2o.reactive.redis

import java.io._
import java.net.InetSocketAddress
import java.util.UUID

import scala.collection.JavaConverters._

sealed trait RedisMode

object RedisMode {

  case object Standalone extends RedisMode

  case object Sentinel extends RedisMode

  case object Cluster extends RedisMode

}

class TestServer(mode: RedisMode = RedisMode.Standalone, portOpt: Option[Int] = None) {
  private[this] var process: Option[Process]      = None
  private[this] val forbiddenPorts                = 6300.until(7300)
  private var _address: Option[InetSocketAddress] = None

  def getPort = portOpt.getOrElse(_address.get.getPort)

  def address: Option[InetSocketAddress] = _address

  val path = "/usr/local/bin/redis-server"

  assertRedisBinaryPresent()
  findAddress()

  private[this] def assertRedisBinaryPresent(): Unit = {
    val p = new ProcessBuilder(path, "--help").start()
    p.waitFor()
    val exitValue = p.exitValue()
    require(exitValue == 0 || exitValue == 1, "redis-server binary must be present.")
  }

  private[this] def findAddress(): InetSocketAddress = {
    var tries = 100
    while (_address.isEmpty && tries >= 0) {
      _address = Some(RandomSocket.nextAddress())
      if (forbiddenPorts.contains(_address.get.getPort)) {
        _address = None
        tries -= 1
        Thread.sleep(5)
      }
    }
    _address.getOrElse {
      sys.error("Couldn't get an address for the external redis instance")
    }
  }

  protected def createConfigFile(port: Int): File = {
    val f = File.createTempFile("redis-" + UUID.randomUUID().toString, ".tmp")
    f.deleteOnExit()
    var out: PrintWriter = null
    try {
      out = new PrintWriter(new BufferedWriter(new FileWriter(f)))
      val confs = Seq(
        "protected-mode no",
        s"port $port"
      )
      confs.foreach { conf =>
        out.write(conf)
        out.println()
      }
    } finally {
      if (out != null)
        out.close()
    }
    f
  }

  def start() {
    val port = getPort
    val conf = createConfigFile(port).getAbsolutePath
    val cmd: Seq[String] = if (mode == RedisMode.Sentinel) {
      Seq(path, conf, "--sentinel")
    } else {
      Seq(path, conf)
    }
    val builder = new ProcessBuilder(cmd.asJava)
    process = Some(builder.start())
    Thread.sleep(200)
  }

  def stop() {
    process.foreach { p =>
      p.destroy()
      p.waitFor()
    }
  }

  def restart() {
    stop()
    start()
  }

}

import java.net.{InetSocketAddress, Socket}

object RandomSocket {

  private[this] def localSocketOnPort(port: Int) =
    new InetSocketAddress(port)

  private[this] val ephemeralSocketAddress = localSocketOnPort(0)

  def apply() = nextAddress()

  def nextAddress(): InetSocketAddress =
    localSocketOnPort(nextPort())

  def nextPort(): Int = {
    val s = new Socket
    s.setReuseAddress(true)
    try {
      s.bind(ephemeralSocketAddress)
      s.getLocalPort
    } catch {
      case e: Throwable =>
        throw new Exception("Couldn't find an open port: %s".format(e.getMessage))
    } finally {
      s.close()
    }
  }
}
