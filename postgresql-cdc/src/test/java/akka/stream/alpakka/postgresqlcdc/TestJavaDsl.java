/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc;


import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.stream.ActorMaterializer;
import akka.stream.Attributes;
import akka.stream.Materializer;
import akka.stream.alpakka.postgresqlcdc.javadsl.ChangeDataCapture;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.TestSink;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

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
    public void exampleConstructSettingsForSource() {
        //#SourceSettings
        final String connectionString = "jdbc:postgresql://localhost:5435/pgdb?user=pguser&password=pguser";

        final PostgreSQLInstance postgreSQLInstance = PostgreSQLInstance
                .create(connectionString, "slot_name");

        final PgCdcSourceSettings changeDataCaptureSettings = PgCdcSourceSettings
                .create()
                .withCreateSlotOnStart(false)
                .withMaxItems(256)
                .withPollInterval(Duration.ofSeconds(7))
                .withMode(Modes.createPeekMode())
                .withColumnsToIgnore(new HashMap<String, List<String>>() {{
                    put("images", Collections.singletonList("binary"));
                    put("user_personal_information", Collections.singletonList("*"));
                    // ignore the binary column in the images table and ignore the user_personal_information table
                }});
        //#SourceSettings
    }

    // for documentation
    public void exampleConstructSettingsForAckSink() {
        //#AckSinkSettings

        final String connectionString = "jdbc:postgresql://localhost:5435/pgdb?user=pguser&password=pguser";

        final PostgreSQLInstance postgreSQLInstance = PostgreSQLInstance
                .create(connectionString, "slot_name");

        final PgCdcAckSinkSettings ackSinkSettings = PgCdcAckSinkSettings
                .create()
                .withMaxItems(16)
                .withMaxItemsWait(Duration.ofSeconds(7));

        //#AckSinkSettings
    }


    // for documentation
    public void exampleGet() {
        //#GetExample

        final ActorSystem system = ActorSystem.create();
        final Materializer materializer = ActorMaterializer.create(system);

        final String connectionString = "jdbc:postgresql://localhost:5435/pgdb?user=pguser&password=pguser";

        final PostgreSQLInstance postgreSQLInstance = PostgreSQLInstance
                .create(connectionString, "slot_name");

        ChangeDataCapture.source(postgreSQLInstance, PgCdcSourceSettings.create())
                .log("postgresqlcdc", changeSet -> String.format("captured changes: %s", changeSet.toString()))
                .withAttributes(Attributes.createLogLevels(Logging.InfoLevel(), Logging.DebugLevel(), Logging.ErrorLevel()))
                .to(Sink.ignore())
                .run(materializer);

        //#GetExample
    }

    // for documentation
    public void examplePeek() {
        //#PeekExample

        class UserRegistered {

            private String userId;

            public UserRegistered(String userId) {
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

        final Source<ChangeSet, NotUsed> source = ChangeDataCapture.source(postgreSQLInstance, PgCdcSourceSettings.create());

        final Sink<ChangeSet, NotUsed> ackSink = ChangeDataCapture.ackSink(postgreSQLInstance, PgCdcAckSinkSettings.create());

        source.filter(changeSet -> changeSet.getChanges().get(0) instanceof RowInserted)
                .filter(changeSet -> changeSet.getChanges().get(0).getTableName().equals("users"))
                .map(changeSet -> {
                        String userId = ((RowInserted)(changeSet.getChanges().get(0))).getFields()
                                .stream()
                                .filter(f -> f.getColumnName().equals("user_id"))
                                .map(Field::getValue)
                                .findFirst()
                                .orElse("unknown");
                        return Tuple2.apply(changeSet, new UserRegistered(userId));
                    })
                .map(result -> result) // do something useful e.g., publish to SQS
                .map(tuple -> tuple._1)
                .to(ackSink)
                .run(materializer);

        //#PeekExample
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
                .source(postgreSQLInstance, PgCdcSourceSettings.create())
                .mapConcat(ChangeSet::getChanges);

        source.runWith(TestSink.probe(system), materializer)
                .request(3)
                .expectNextN(3);

    }

}

