/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

/*
 * Start package with 'docs' prefix when testing APIs as a user.
 * This prevents any visibility issues that may be hidden.
 */
package docs.javadsl;

import akka.NotUsed;
import akka.stream.alpakka.reference.Authentication;
import akka.stream.alpakka.reference.ReferenceReadMessage;
import akka.stream.alpakka.reference.ReferenceWriteMessage;
import akka.stream.alpakka.reference.SourceSettings;
import akka.stream.alpakka.reference.javadsl.Reference;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletionStage;

/**
 * Append "Test" to every Java test suite.
 */
class ReferenceTest {

  /**
   * Called before every test.
   */
  @Before
  public void setUp() {

  }

  @Test
  public void settingsCompilationTest() {
    final Authentication.Provided providedAuth =
      Authentication.createProvided().withVerifierPredicate(c -> true);

    final Authentication.None noAuth =
      Authentication.createNone();

    final SourceSettings settings = SourceSettings.create();

    settings.withAuthentication(providedAuth);
    settings.withAuthentication(noAuth);
  }

  @Test
  public void sourceCompilationTest() {
    // #source
    final SourceSettings settings = SourceSettings.create();

    final Source<ReferenceReadMessage, CompletionStage<NotUsed>> source =
      Reference.source(settings);
    // #source
  }

  @Test
  public void flowCompilationTest() {
    // #flow
    final Flow<ReferenceWriteMessage, ReferenceWriteMessage, NotUsed> flow =
      Reference.flow();
    // #flow
  }

  /**
   * Called after every test.
   */
  @After
  public void tearDown() {

  }

}