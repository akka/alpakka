/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.stream.alpakka.geode.AkkaPdxSerializer;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxWriter;

// #animal-pdx-serializer
public class AnimalPdxSerializer implements AkkaPdxSerializer<Animal> {
  @Override
  public Class<Animal> clazz() {
    return Animal.class;
  }

  @Override
  public boolean toData(Object o, PdxWriter out) {
    if (o instanceof Animal) {
      Animal p = (Animal) o;
      out.writeInt("id", p.getId());
      out.writeString("name", p.getName());
      out.writeInt("owner", p.getOwner());
      return true;
    }
    return false;
  }

  @Override
  public Object fromData(Class<?> clazz, PdxReader in) {
    int id = in.readInt("id");
    String name = in.readString("name");
    int owner = in.readInt("owner");
    return new Animal(id, name, owner);
  }
}
// #animal-pdx-serializer
