/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import java.util.Objects;

// #pojo-domain-object
public final class DomainObject {
  private Integer id;
  private String firstProperty;
  private String secondProperty;

  public DomainObject() {}

  public DomainObject(Integer id, String firstProperty, String secondProperty) {
    this.id = id;
    this.firstProperty = firstProperty;
    this.secondProperty = secondProperty;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getFirstProperty() {
    return firstProperty;
  }

  public void setFirstProperty(String firstProperty) {
    this.firstProperty = firstProperty;
  }

  public String getSecondProperty() {
    return secondProperty;
  }

  public void setSecondProperty(String secondProperty) {
    this.secondProperty = secondProperty;
  }

  // #pojo-domain-object
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DomainObject that = (DomainObject) o;
    return Objects.equals(id, that.id)
        && Objects.equals(firstProperty, that.firstProperty)
        && Objects.equals(secondProperty, that.secondProperty);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, firstProperty, secondProperty);
  }

  @Override
  public String toString() {
    return "DomainObject{"
        + "id="
        + id
        + ", firstProperty='"
        + firstProperty
        + '\''
        + ", secondProperty='"
        + secondProperty
        + '\''
        + '}';
  }
  // #pojo-domain-object
}
// #pojo-domain-object
