/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import java.util.Date;

public class Person {
  private final int id;
  private final String name;
  private final Date birthDate;

  public Person(int id, String name, Date birthDate) {
    this.id = id;
    this.name = name;
    this.birthDate = birthDate;
  }

  public int getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public Date getBirthDate() {
    return birthDate;
  }

  @Override
  public String toString() {
    return getId() + ": " + getName();
  }
}
