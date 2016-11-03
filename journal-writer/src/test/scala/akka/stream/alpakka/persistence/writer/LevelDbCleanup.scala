/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.persistence.writer

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll

trait LevelDbCleanup { _: TestSpec with BeforeAndAfterAll =>
  def storageLocations = List(
    "akka.persistence.journal.leveldb.dir"
  ).map(s => new File(system.settings.config.getString(s)))

  override protected def beforeAll(): Unit = {
    storageLocations.foreach(FileUtils.deleteDirectory)
  }
}
