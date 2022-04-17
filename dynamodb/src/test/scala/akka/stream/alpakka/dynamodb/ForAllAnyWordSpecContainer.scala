/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import com.dimafeng.testcontainers.ForAllTestContainer
import org.scalatest.{Args, Status, Suite}
import org.scalatest.wordspec.AnyWordSpecLike

trait ForAllAnyWordSpecContainer extends AnyWordSpecLike with ForAllTestContainer { self: Suite =>
  override abstract def runTest(testName: String, args: Args): Status = {
    super.runTest(testName, args)
  }

  override abstract def run(testName: Option[String], args: Args): Status = {
    super.run(testName, args)
  }
}
