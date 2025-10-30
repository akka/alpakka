/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.csv.javadsl;

/** Select which fields to quote in CSV formatting. */
public enum CsvQuotingStyle {
  /** Quote all fields */
  ALWAYS,

  /** Quote only fields requiring quotes */
  REQUIRED;
}
