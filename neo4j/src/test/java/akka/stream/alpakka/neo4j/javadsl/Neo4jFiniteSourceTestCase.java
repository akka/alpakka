/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.javadsl;

import akka.Done;
import akka.stream.alpakka.neo4j.internal.CypherUnmarshaller;
import org.junit.Test;
import org.neo4j.driver.v1.Value;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;


public class Neo4jFiniteSourceTestCase extends Neo4jBaseTestCase {




    @Test
    public void testFiniteSource() throws ExecutionException, InterruptedException {

        //#source

        //#unmarshaller
        CypherUnmarshaller<Person> unmarshaller = new CypherUnmarshaller<Person>() {
            @Override
            public Person unmarshall(Value record) {
                return new Person(record.get("name", ""), record.get("height", 0), record.get("birthDate",""));
            }
        };
        //#unmarshaller


        ReactiveNeo4j reactiveNeo4j = new ReactiveNeo4j(driver);

        CompletionStage<Done> personsDone = reactiveNeo4j.source(unmarshaller, "MATCH (ee:Person) RETURN ee;")
                .runForeach(p -> {
                    LOGGER.debug(p.toString());
                }, materializer);
        //#source

        personsDone.toCompletableFuture().get();
    }

}
