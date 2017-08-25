/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.javadsl;


import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.testkit.JavaTestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jBaseTestCase {
    protected static final Logger LOGGER = LoggerFactory.getLogger(Neo4jBaseTestCase.class);


    //#connect
    protected Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "test"));
    //#connect


    private static ActorSystem system;
    protected static Materializer materializer;


    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);

    }


    @AfterClass
    public static void teardown() {
        JavaTestKit.shutdownActorSystem(system);
    }

}
