/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.postgresqlcdc.javadsl.*;
import akka.stream.testkit.javadsl.TestSink;
import akka.testkit.javadsl.TestKit;
import akka.stream.javadsl.Source;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class TestJavaDsl {

    static ActorSystem system;
    static Materializer materializer;
    static Connection connection;
    static final String connectionString = "jdbc:postgresql://localhost/pgdb2?user=pguser&password=pguser";

    private static void createUsersTable() throws Exception {
        connection.prepareStatement("CREATE TABLE users (id SERIAL NOT NULL PRIMARY KEY, info TEXT NOT NULL);").execute();
    }

    private static void dropUsersTable() throws Exception {
        connection.prepareStatement("DROP TABLE users;").execute();
    }

    private static void insertUser(int id, String info) throws Exception {
        PreparedStatement st = connection.prepareStatement("INSERT INTO users(id, info) VALUES(?, ?);");
        st.setInt(1, id);
        st.setString(2, info);
        st.execute();
    }

    private static void updateUser(int id, String newInfo) throws Exception {
        PreparedStatement st = connection.prepareStatement("UPDATE users SET info = ? WHERE id = ?;");
        st.setString(1, newInfo);
        st.setInt(2, id);
        st.execute();
    }

    private static void deleteUsers() throws Exception {
        connection.prepareStatement("DELETE FROM users;").execute();
    }

    @BeforeClass
    public static void setup() throws Exception {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
        String driver = "org.postgresql.Driver";
        Class.forName(driver).newInstance();
        connection = DriverManager.getConnection(connectionString);
        // set the logical replication slot
        connection.prepareStatement("SELECT * FROM pg_create_logical_replication_slot('junit','test_decoding')").execute();
        createUsersTable();
    }

    @AfterClass
    public static void teardown() throws Exception {
        TestKit.shutdownActorSystem(system);
        system = null;
        materializer = null;
        dropUsersTable();
        // drop the logical replication slot
        connection.prepareStatement("SELECT * FROM pg_drop_replication_slot('junit')").execute();
        connection.close();
    }

    @Test
    public void testIt() throws Exception {

        insertUser(1, "freemium user");
        updateUser(1, "premium user");
        deleteUsers();

        final PostgreSQLInstance postgreSQLInstance = new PostgreSQLInstance(connectionString,
                "junit",
                Plugin.TestDecoding,
                128,
                1000
        );

        final Source<Change, NotUsed> source = ChangeDataCapture
                .from(postgreSQLInstance)
                .mapConcat(ChangeSet::getChanges);

        source.runWith(TestSink.probe(system), materializer)
                .request(3)
                .expectNextN(3); // TODO: improve test

    }

}
