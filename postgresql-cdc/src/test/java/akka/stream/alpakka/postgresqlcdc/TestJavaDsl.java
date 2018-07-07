/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc;


import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.postgresqlcdc.javadsl.ChangeDataCapture;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.TestSink;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import akka.event.Logging;
import akka.stream.Attributes;

public class TestJavaDsl {

    private static ActorSystem system;
    private static Materializer materializer;
    private static Connection connection;

    private static final String connectionString = "jdbc:postgresql://localhost:5435/pgdb2?user=pguser&password=pguser";

    @BeforeClass
    public static void setup() throws Exception {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
        String driver = "org.postgresql.Driver";
        Class.forName(driver).newInstance();
        connection = FakeDb.getConnection(connectionString);
        // set the logical replication slot
        FakeDb.setUpLogicalDecodingSlot("junit", connection);
        FakeDb.createCustomersTable(connection);
    }

    @AfterClass
    public static void teardown() throws Exception {
        TestKit.shutdownActorSystem(system);
        system = null;
        materializer = null;
        FakeDb.dropTableCustomers(connection);
        // drop the logical replication slot
        FakeDb.dropLogicalDecodingSlot("junit", connection);
        connection.close();
    }


    // for documentation
    public void exampleConstructSettings() {
        //#ChangeDataCaptureSettings
        final String connectionString = "jdbc:postgresql://localhost:5435/pgdb?user=pguser&password=pguser";

        final PostgreSQLInstance postgreSQLInstance = PostgreSQLInstance
                .create(connectionString, "slot_name");

        final ChangeDataCaptureSettings changeDataCaptureSettings = ChangeDataCaptureSettings
                .create()
                .withCreateSlotOnStart(false)
                .withMaxItems(256)
                .withPollInterval(Duration.ofSeconds(7))
                .withMode(Modes.choosePeekMode())
                .withTablesToIgnore(Collections.singletonList("user_personal_information")) // ignore the user_personal_information table
                .withColumnsToIgnore(new HashMap<String, List<String>>() {{
                    put("images", Collections.singletonList("binary")); // ignore the binary column in the images table
                }});
        //#ChangeDataCaptureSettings

    }

    // for documentation
    public void exampleBasic() {
        //#BasicExample

        final ActorSystem system = ActorSystem.create();
        final Materializer materializer = ActorMaterializer.create(system);

        final String connectionString = "jdbc:postgresql://localhost:5435/pgdb?user=pguser&password=pguser";

        final PostgreSQLInstance postgreSQLInstance = PostgreSQLInstance
                .create(connectionString, "slot_name");

        ChangeDataCapture
                .source(postgreSQLInstance, ChangeDataCaptureSettings.create())
                .log("postgresqlcdc", changeSet -> String.format("captured changes: %s", changeSet.toString()))
                .withAttributes(Attributes.createLogLevels(Logging.InfoLevel(), Logging.DebugLevel(), Logging.ErrorLevel()))
                .to(Sink.ignore())
                .run(materializer);

        //#BasicExample
    }

    // for documentation
    public void exampleProcessEvents() {
        //#ProcessEventsExample

        class UserDeregistered {

            private String userId;

            public UserDeregistered(String userId) {
                this.userId = userId;
            }

            public String getUserId() {
                return userId;
            }
        }

        final ActorSystem system = ActorSystem.create();
        final Materializer materializer = ActorMaterializer.create(system);

        final String connectionString = "jdbc:postgresql://localhost:5435/pgdb?user=pguser&password=pguser";

        final PostgreSQLInstance postgreSQLInstance = PostgreSQLInstance
                .create(connectionString, "slot_name");

        ChangeDataCapture
                .source(postgreSQLInstance, ChangeDataCaptureSettings.create())
                .mapConcat(ChangeSet::getChanges)
                .collectType(RowDeleted.class)
                .map(deletedRow -> {
                  String userId = deletedRow.getFields()
                          .stream()
                          .filter(f -> f.getColumnName().equals("user_id"))
                          .map(Field::getValue)
                          .findFirst()
                          .orElse("unknown");
                  return new UserDeregistered(userId);
                })
                .to(Sink.ignore())
                .run(materializer);
        //#ProcessEventsExample
    }

    @Test
    public void testIt() {
        Instant time = Instant.parse("1964-02-23T00:00:00Z");

        // some inserts
        FakeDb.insertCustomer(0, "John", "Lennon", "john.lennon@akka.io", new ArrayList(), time, connection);
        // some updates
        FakeDb.updateCustomerEmail(0, "john.lennon@thebeatles.com", connection);
        // some deletes
        FakeDb.deleteCustomers(connection);

        final PostgreSQLInstance postgreSQLInstance = PostgreSQLInstance
                .create(connectionString, "junit");

        final Source<Change, NotUsed> source = ChangeDataCapture
                .source(postgreSQLInstance, ChangeDataCaptureSettings.create())
                .mapConcat(ChangeSet::getChanges);

        source.runWith(TestSink.probe(system), materializer)
                .request(3)
                .expectNextN(3); // TODO: improve test

    }

}

