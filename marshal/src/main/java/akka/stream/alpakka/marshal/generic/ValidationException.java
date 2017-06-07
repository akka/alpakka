package akka.stream.alpakka.marshal.generic;

/**
 * Thrown by readers when the encountered structure does not match the expected structure, e.g. missing fields, wrong format, etc.
 */
@SuppressWarnings("serial")
public class ValidationException extends IllegalArgumentException {

    public ValidationException() {
        super();
    }

    public ValidationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ValidationException(String message) {
        super(message);
    }

    public ValidationException(Throwable cause) {
        super(cause);
    }
}
