package com.github.j5ik2o.reactive.redis

import java.io._
import java.net.InetSocketAddress
import java.util.UUID

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

sealed trait RedisMode

object RedisMode {

  case object Standalone extends RedisMode

  case object Sentinel extends RedisMode

  case object Cluster extends RedisMode

}

class TestServer(mode: RedisMode = RedisMode.Standalone, portOpt: Option[Int] = None)
    extends RandomPortSupport {
  private[this] var process: Option[Process]      = None
  private[this] val forbiddenPorts                = 6300.until(7300)
  private var _address: Option[InetSocketAddress] = None

  def getPort = portOpt.getOrElse(_address.get.getPort)

  def address: Option[InetSocketAddress] = _address

  val path = sys.env.getOrElse("REDIS_SERVER_PATH", "/usr/local/bin/redis-server")

  private[this] def assertRedisBinaryPresent()(implicit ec: ExecutionContext): Unit = {
    val p = new ProcessBuilder(path, "--help").start()
    printlnStreamFuture(new BufferedReader(new InputStreamReader(p.getInputStream)))
    printlnStreamFuture(new BufferedReader(new InputStreamReader(p.getErrorStream)))
    p.waitFor()
    val exitValue = p.exitValue()
    require(exitValue == 0 || exitValue == 1, "redis-server binary must be present.")
  }

  private[this] def findAddress(): InetSocketAddress = {
    var tries = 100
    while (_address.isEmpty && tries >= 0) {
      _address = Some(temporaryServerAddress())
      if (forbiddenPorts.contains(_address.get.getPort)) {
        _address = None
        tries -= 1
        println("try to get port...")
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

  def printlnStreamFuture(br: BufferedReader)(implicit ec: ExecutionContext): Future[Unit] = {
    val result = Future {
      br.readLine()
    }.flatMap { result =>
      if (result != null) {
        println(result)
        printlnStreamFuture(br)
      } else
        Future.successful(())
    }.recoverWith {
      case ex =>
        Future.successful(())
    }
    result.onComplete {
      case Success(_) =>
        br.close()
      case Failure(ex) =>
        ex.printStackTrace()
        br.close()
    }
    result
  }

  def start()(implicit ec: ExecutionContext) {
    assertRedisBinaryPresent()
    findAddress()
    val port = getPort
    val conf = createConfigFile(port).getAbsolutePath
    val cmd: Seq[String] = if (mode == RedisMode.Sentinel) {
      Seq(path, conf, "--sentinel")
    } else {
      Seq(path, conf)
    }
    val builder  = new ProcessBuilder(cmd.asJava)
    val _process = builder.start()
    printlnStreamFuture(new BufferedReader(new InputStreamReader(_process.getInputStream)))
    printlnStreamFuture(new BufferedReader(new InputStreamReader(_process.getErrorStream)))
    process = Some(_process)
    Thread.sleep(200)
  }

  def stop(): Unit = {
    process.foreach { p =>
      p.destroy()
      p.waitFor()
    }
  }

  def restart()(implicit ec: ExecutionContext): Unit = {
    stop()
    start()
  }

}

import java.net.InetSocketAddress
import java.nio.channels.ServerSocketChannel

/**
  * This code is originated from Spray.
  * https://github.com/spray/spray/blob/b473d9e8ce503bafc72825914f46ae6be1588ce7/spray-util/src/main/scala/spray/util/Utils.scala#L35-L47
  */
trait RandomPortSupport {

  def temporaryServerAddress(interface: String = "127.0.0.1"): InetSocketAddress = {
    val serverSocket = ServerSocketChannel.open()
    try {
      serverSocket.socket.bind(new InetSocketAddress(interface, 0))
      val port = serverSocket.socket.getLocalPort
      new InetSocketAddress(interface, port)
    } finally serverSocket.close()
  }

  def temporaryServerHostnameAndPort(interface: String = "127.0.0.1"): (String, Int) = {
    val socketAddress = temporaryServerAddress(interface)
    socketAddress.getHostName -> socketAddress.getPort
  }

  def temporaryServerPort(interface: String = "127.0.0.1"): Int =
    temporaryServerHostnameAndPort(interface)._2
}
