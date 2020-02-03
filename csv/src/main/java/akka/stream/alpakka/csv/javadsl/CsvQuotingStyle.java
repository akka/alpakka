/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.csv.javadsl;

/** Select which fields to quote in CSV formatting. */
public enum CsvQuotingStyle {
  /** Quote all fields */
  ALWAYS,

  /** Quote only fields requiring quotes */
  REQUIRED;
}
