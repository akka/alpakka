/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package docs.javadsl;

public class Animal {
  private final int id;
  private final String name;
  private final int owner;

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
    return getId() + ": " + getName() + " owner: " + getOwner();
  }
}
