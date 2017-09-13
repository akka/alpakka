/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.ibm.eventstore.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.ibm.eventstore.EventStoreConfiguration;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import com.ibm.event.catalog.TableSchema;
import com.ibm.event.common.ConfigurationReader;
import com.ibm.event.oltp.EventContext;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;


/**
 * This unit test is run using a local installation of EventStore
 * The installer for EventStore can be obtained from:
 * https://www.ibm.com/us-en/marketplace/project-eventstore
 */
@Ignore
public class EventStoreSpec {
    static ActorSystem system;
    static Materializer materializer;

    // #configuration
    private static EventStoreConfiguration configuration = EventStoreConfiguration.apply(ConfigFactory.load());
    // #configuration

    private static Pair<ActorSystem, Materializer> setupMaterializer() {
        final ActorSystem system = ActorSystem.create();
        final Materializer materializer = ActorMaterializer.create(system);
        return Pair.create(system, materializer);
    }

    private static TableSchema getTableSchema() {
        Seq pkList = JavaConversions.asScalaBuffer(Collections.singletonList("id")).toSeq();
        List<StructField> fields = new ArrayList<>();

        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("someInt", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("someString", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("someBoolean", DataTypes.BooleanType, false));
        fields.add(DataTypes.createStructField("someOtherBoolean", DataTypes.BooleanType, true));

        return new TableSchema(configuration.tableName(), DataTypes.createStructType(fields), pkList, pkList, scala.Option.apply(null));
    }

    @BeforeClass
    public static void setup() {
        final Pair<ActorSystem, Materializer> sysmat = setupMaterializer();
        system = sysmat.first();
        materializer = sysmat.second();

        // #configure-endpoint
        ConfigurationReader.setConnectionEndpoints(configuration.endpoint());
        // #configure-endpoint
        EventContext.dropDatabase(configuration.databaseName());
        EventContext context = EventContext.createDatabase("TESTDB");
        TableSchema reviewSchema = getTableSchema();
        context.createTable(reviewSchema);
    }

    @AfterClass
    public static void teardown() {
        ConfigurationReader.setConnectionEndpoints(configuration.endpoint());
        EventContext.dropDatabase(configuration.databaseName());
        // #cleanup
        EventContext.cleanUp();
        // #cleanup
        JavaTestKit.shutdownActorSystem(system);
    }

    @Test
    public void testInserting() throws Exception {

        //#insert-rows
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, 1, "Hello", true, false));
        rows.add(RowFactory.create(2, 1, "Hello", true, false));
        rows.add(RowFactory.create(3, 1, "Hello", true, false));

        Sink<Row, CompletionStage<Done>> sink = EventStoreSink.create(configuration, materializer.executionContext());
        final CompletionStage<Done> insertionResultFuture = Source.from(rows).runWith(sink, materializer);
        //#insert-rows

        insertionResultFuture.toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

}
