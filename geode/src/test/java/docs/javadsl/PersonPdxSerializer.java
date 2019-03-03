/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.stream.alpakka.geode.AkkaPdxSerializer;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxWriter;

import java.util.Date;

// #person-pdx-serializer
public class PersonPdxSerializer implements AkkaPdxSerializer<Person> {

  @Override
  public Class<Person> clazz() {
    return Person.class;
  }

  @Override
  public boolean toData(Object o, PdxWriter out) {
    if (o instanceof Person) {
      Person p = (Person) o;
      out.writeInt("id", p.getId());
      out.writeString("name", p.getName());
      out.writeDate("birthDate", p.getBirthDate());
      return true;
    }
    return false;
  }

  @Override
  public Object fromData(Class<?> clazz, PdxReader in) {
    int id = in.readInt("id");
    String name = in.readString("name");
    Date birthDate = in.readDate("birthDate");
    return new Person(id, name, birthDate);
  }
}
// #person-pdx-serializer
