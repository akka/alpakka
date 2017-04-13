/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.druid

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId}

object TimeUtils {

  /**
   * This ISO date format is expected ...
   */
  private val isoDateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  private val isoDateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'H:m:s'Z'")

  private val gmtZone = ZoneId.of("GMT")

  def timestampToGMT(instant: Instant): LocalDateTime = LocalDateTime.ofInstant(instant, gmtZone)

  def timestamp2ISODate(instant: Instant): String = timestampToGMT(instant).format(isoDateFormat)

  def timestamp2ISODateTime(instant: Instant): String = timestampToGMT(instant).format(isoDateTimeFormat)

}
