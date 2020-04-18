/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.debezium.impl;

import io.debezium.engine.DebeziumEngine;

import java.util.List;

public interface LazyDebeziumEngine<T> extends DebeziumEngine<T> {
  List<T> poll();

  RecordCommitter<T> committer();
}
