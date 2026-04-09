/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka
package azure
package storage

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Clock, Instant, ZoneOffset}

class FormattedDateSpec extends AnyFlatSpec with Matchers {

  // Regression test for a real Azure SharedKey auth failure: Azure Storage rejects requests
  // with `AuthenticationErrorDetail: The Date header in the request is incorrect.` when the
  // x-ms-date header uses a single-digit day-of-month. Java's DateTimeFormatter.RFC_1123_DATE_TIME
  // emits a single-digit day for days 1-9 (e.g. "Tue, 7 Apr 2026 ..."), which is not strict
  // RFC 7231 IMF-fixdate. We must always emit two digits.

  "getFormattedDate" should "use a two-digit day for days 1-9 (RFC 7231 IMF-fixdate)" in {
    // 2026-04-07 — day 7, would be a single digit with RFC_1123_DATE_TIME
    implicit val clock: Clock = Clock.fixed(Instant.parse("2026-04-07T15:10:38Z"), ZoneOffset.UTC)
    getFormattedDate shouldBe "Tue, 07 Apr 2026 15:10:38 GMT"
  }

  it should "use a two-digit day for days 10-31" in {
    implicit val clock: Clock = Clock.fixed(Instant.parse("2026-04-17T15:10:38Z"), ZoneOffset.UTC)
    getFormattedDate shouldBe "Fri, 17 Apr 2026 15:10:38 GMT"
  }

  it should "always use English month/day names regardless of default locale" in {
    val previousLocale = java.util.Locale.getDefault
    try {
      java.util.Locale.setDefault(java.util.Locale.forLanguageTag("fr-FR"))
      implicit val clock: Clock = Clock.fixed(Instant.parse("2026-04-07T15:10:38Z"), ZoneOffset.UTC)
      getFormattedDate shouldBe "Tue, 07 Apr 2026 15:10:38 GMT"
    } finally {
      java.util.Locale.setDefault(previousLocale)
    }
  }
}
