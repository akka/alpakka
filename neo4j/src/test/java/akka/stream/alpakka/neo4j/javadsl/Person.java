/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.neo4j.javadsl;

public class Person {
    public String getName() {
        return name;
    }

    public int getHeight() {
        return height;
    }

    public String getBirthDate() {
        return birthDate;
    }

    private String name;
    private int height;
    private String birthDate;

    public Person(String name, int height, String birthDate) {
        this.name = name;
        this.height = height;
        this.birthDate = birthDate;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", height=" + height +
                ", birthDate='" + birthDate + '\'' +
                '}';
    }
}
