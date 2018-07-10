package com.github.j5ik2o.reactive.redis

import akka.testkit.TestKit

trait TimeFactorSupport { self: TestKit =>

  lazy val timeFactor: Double = testKitSettings.TestTimeFactor

}
