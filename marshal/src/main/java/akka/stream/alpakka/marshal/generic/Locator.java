package akka.stream.alpakka.marshal.generic;

/**
 * Returns a human-readable parsing location for inclusion with errors reported when
 * handling a particular event of type E.
 */
public interface Locator<E> {
    public String getLocation(E e);
}
