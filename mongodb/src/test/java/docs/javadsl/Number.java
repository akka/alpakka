/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import java.util.Objects;

// #pojo
public final class Number {
  private Integer _id;

  public Number() {}

  public Number(Integer _id) {
    this._id = _id;
  }

  public void setId(Integer _id) {
    this._id = _id;
  }

  public Integer getId() {
    return _id;
  }

  // #pojo
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Number number = (Number) o;
    return Objects.equals(_id, number._id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_id);
  }

  @Override
  public String toString() {
    return "Number{" + "_id=" + _id + '}';
  }
  // #pojo
}
// #pojo
