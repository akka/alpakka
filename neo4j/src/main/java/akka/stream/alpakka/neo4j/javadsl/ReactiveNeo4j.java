/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.stream.alpakka.neo4j.internal.CypherMarshaller;
import akka.stream.alpakka.neo4j.internal.CypherUnmarshaller;
import akka.stream.alpakka.neo4j.internal.Neo4jCapabilities;
import akka.stream.alpakka.neo4j.internal.stage.Neo4jFiniteSourceStage;
import akka.stream.alpakka.neo4j.internal.stage.Neo4jFlowStage;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Values;
import scala.concurrent.Future;


public class ReactiveNeo4j extends Neo4jCapabilities {


    public ReactiveNeo4j(Driver driver) {
        super(driver);
    }

    public <A> Source<A, Future<Done>> source(CypherUnmarshaller<A> readHelper, Statement statement) {
        return Source.fromGraph(new Neo4jFiniteSourceStage<A>(driver(), readHelper, statement));
    }

    public <A> Source<A, Future<Done>> source(CypherUnmarshaller<A> readHelper, String query, Object ... args) {
        return source(readHelper, new Statement(query, Values.parameters(args)));
    }


    public <A> Flow<A,A,NotUsed> flow(String label, CypherMarshaller<A> writerHelper) {
        return Flow.fromGraph(new Neo4jFlowStage<A>(driver(), writerHelper));
    }
}
