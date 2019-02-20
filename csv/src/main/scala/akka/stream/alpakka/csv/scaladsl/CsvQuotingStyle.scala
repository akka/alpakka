/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.csv.scaladsl

import akka.stream.alpakka.csv.javadsl

sealed trait CsvQuotingStyle

/**
 * Select which fields to quote in CSV formatting.
 */
object CsvQuotingStyle {

  /** Quote only fields requiring quotes */
  case object Required extends CsvQuotingStyle

  /** Quote all fields */
  case object Always extends CsvQuotingStyle

  /** Java to Scala conversion helper */
  def asScala(qs: javadsl.CsvQuotingStyle): CsvQuotingStyle = qs match {
    case javadsl.CsvQuotingStyle.ALWAYS => CsvQuotingStyle.Always
    case javadsl.CsvQuotingStyle.REQUIRED => CsvQuotingStyle.Required
  }

}
