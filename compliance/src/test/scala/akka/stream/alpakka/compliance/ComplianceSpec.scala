/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.compliance

import org.scalatest.{Matchers, WordSpec}
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.library.GeneralCodingRules._

class ComplianceSpec extends WordSpec with Matchers {

  val rules = Set(NO_CLASSES_SHOULD_USE_JAVA_UTIL_LOGGING,
                  NO_CLASSES_SHOULD_ACCESS_STANDARD_STREAMS,
                  NO_CLASSES_SHOULD_USE_JODATIME)

  val classes = new ClassFileImporter().importPackages("akka.stream.alpakka")

  "Alpakka classes" should {
    "comply with [archunit] rules" in {
      classes.size shouldBe >(0)
      rules.foreach { _.check(classes) }
    }
  }
}
