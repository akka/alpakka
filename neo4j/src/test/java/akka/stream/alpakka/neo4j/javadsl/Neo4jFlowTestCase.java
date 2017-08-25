/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.javadsl;

import akka.NotUsed;
import akka.stream.alpakka.neo4j.internal.CypherMarshaller;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;


public class Neo4jFlowTestCase extends Neo4jBaseTestCase {



    static Source<Person, NotUsed> buildPersonsSource(Integer... ids) {
        return Source.from(Arrays.asList(ids))
                .map((i) -> new Person(String.format("Person Java %d", i), 150 + i, "date"));
    }

    @Test
    public void flow() throws ExecutionException, InterruptedException {

        ReactiveNeo4j reactiveNeo4j = new ReactiveNeo4j(driver);

        Source<Person, NotUsed> source = buildPersonsSource(110, 111, 113, 114, 115);

        //#flow

        //#marshaller
        CypherMarshaller<Person> marshaller = new CypherMarshaller<Person>() {

            private String format = "CREATE (o:Person { name: '%s', height: %d, birthDate: '%s' }) RETURN o";

            @Override
            public String create(Person person) {
                return String.format(format, person.getName(), person.getHeight(), person.getBirthDate());
            }
        };
        //#marshaller



        Flow<Person, Person, NotUsed> flow = reactiveNeo4j.flow("Person", marshaller);

        CompletionStage<List<Person>> run = source
                .via(flow)
                .toMat(Sink.seq(), Keep.right())
                .run(materializer);
        //#flow

        run.toCompletableFuture().get();

        reactiveNeo4j.close();

    }


}