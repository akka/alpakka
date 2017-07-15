/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.geode.GeodeSettings;
import akka.stream.alpakka.geode.RegionSettings;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;

import static scala.compat.java8.JFunction.func;

public class GeodeBaseTestCase {


    protected static final Logger LOGGER = LoggerFactory.getLogger(GeodeFlowTestCase.class);

    private static ActorSystem system;
    protected static Materializer materializer;
    private String geodeDockerHostname="localhost";

    {
        String geodeItHostname=System.getenv("IT_GEODE_HOSTNAME");
        if(geodeItHostname!=null)
            geodeDockerHostname=geodeItHostname;
    }

    //#region
    protected RegionSettings<Integer, Person> personRegionSettings = new RegionSettings<>("persons", func(Person::getId));
    protected RegionSettings<Integer, Animal> animalRegionSettings = new RegionSettings<>("animals", func(Animal::getId));
    //#region

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);

    }

    static Source<Person, NotUsed> buildPersonsSource(Integer... ids) {
        return Source.from(Arrays.asList(ids))
                .map((i) -> new Person(i, String.format("Person Java %d", i), new Date()));
    }

    static Source<Animal, NotUsed> buildAnimalsSource(Integer... ids) {
        return Source.from(Arrays.asList(ids))
                .map((i) -> new Animal(i, String.format("Animal Java %d", i), 1));
    }

    protected ReactiveGeode createReactiveGeode() {
        //#connection
        GeodeSettings settings = GeodeSettings.create(geodeDockerHostname, 10334)
                .withConfiguration(func(c->c.setPoolIdleTimeout(10)));
        return new ReactiveGeode(settings);
        //#connection
    }

    protected ReactiveGeodeWithPoolSubscription createReactiveGeodeWithPoolSubscription() {
        GeodeSettings settings = GeodeSettings.create(geodeDockerHostname, 10334);
        //#connection-with-pool
        return new ReactiveGeodeWithPoolSubscription(settings);
        //#connection-with-pool
    }

    @AfterClass
    public static void teardown() {
        JavaTestKit.shutdownActorSystem(system);
    }

}
