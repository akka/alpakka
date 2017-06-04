/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.geode.javadsl;

public class Animal {
    final private int id;
    final private String name;
    final private int owner;


    public Animal(int id, String name, int owner) {
        this.id = id;
        this.name = name;
        this.owner = owner;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getOwner() {
        return owner;
    }

    @Override
    public String toString() {
        return getId() +": " + getName() + " owner: " + getOwner();
    }
}