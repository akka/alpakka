/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.kairosdb.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.TestSource;
import akka.testkit.JavaTestKit;
import org.junit.Before;
import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.QueryMetric;
import org.kairosdb.client.response.GetResponse;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Response;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.kairosdb.client.builder.TimeUnit.*;

import static org.junit.Assert.assertTrue;
import static org.kairosdb.client.builder.TimeUnit.MINUTES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Created by SOROOSH on 3/22/17.
 */
public class KairosSinkTest {

    private static ActorSystem system;
    private static ActorMaterializer materializer;

    private static HttpClient client;

    @BeforeClass
    public static void setup() throws MalformedURLException {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
        client = new HttpClient("http://127.0.0.1:9090");

    }

    @AfterClass
    public static void tearDown() {
        JavaTestKit.shutdownActorSystem(system);
    }


    @Test
    public void sendMetric() throws IOException, URISyntaxException, TimeoutException, InterruptedException {
        MetricBuilder builder = MetricBuilder.getInstance();
        builder.addMetric("M1").addDataPoint(1).addTag("T", "TV");

        Future<Done> doneFuture = Source.single(builder).runWith(KairosSink.create(client, system.dispatcher()), materializer);
        Await.ready(doneFuture, new FiniteDuration(1, TimeUnit.SECONDS));

        assertTrue(client.getTagNames().getResults().contains("T"));
        assertTrue(client.getMetricNames().getResults().contains("M1"));

    }
}
