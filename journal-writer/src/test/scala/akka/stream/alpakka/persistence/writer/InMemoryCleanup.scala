/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.persistence.writer

import akka.pattern.ask
import akka.persistence.inmemory.extension.InMemoryJournalStorage.ClearJournal
import akka.persistence.inmemory.extension.StorageExtension
import org.scalatest.BeforeAndAfterEach

trait InMemoryCleanup { _: TestSpec with BeforeAndAfterEach =>
  override protected def beforeEach(): Unit = {
    (StorageExtension(system).journalStorage ? ClearJournal).toTry should be a 'success
  }
}
