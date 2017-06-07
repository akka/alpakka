package akka.stream.alpakka.marshal.generic;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Function;

import javaslang.control.Try;

/**
 * Converts arbitrary types to and from String
 */
public abstract class StringMarshallable<T> {
    public abstract Try<T> tryRead(String value);
    
    public abstract String write(T value);

    public static final StringMarshallable<String> STRING = new StringMarshallable<String>() {
        @Override
        public Try<String> tryRead(String value) {
            return Try.success(value);
        }

        @Override
        public String write(String value) {
            return value;
        }
        
        @Override
        public String toString() {
            return "string";
        }
    };

    /** Java Long converter. For JSON, please prefer to use {@link JSONProtocol.longValue} since it maps to JSON number instead of string. */
    public static final StringMarshallable<Long> LONG = new StringMarshallable<Long>() {
        @Override
        public Try<Long> tryRead(String value) {
            return Try.of(() -> Long.parseLong(value)).recover(prefixMessage("Expecting a signed 64-bit decimal integer "));
        }

        @Override
        public String write(Long value) {
            return String.valueOf(value);
        }
        
        @Override
        public String toString() {
            return "long";
        }
    };
    
    /** Java Integer converter. For JSON, please prefer to use {@link JSONProtocol.integerValue} since it maps to JSON number instead of string. */
    public static final StringMarshallable<Integer> INTEGER = new StringMarshallable<Integer>() {
        @Override
        public Try<Integer> tryRead(String value) {
            return Try.of(() -> Integer.parseInt(value)).recover(prefixMessage("Expecting a signed 32-bit decimal integer "));
        }

        @Override
        public String write(Integer value) {
            return String.valueOf(value);
        }
        
        @Override
        public String toString() {
            return "integer";
        }
    };
    
    /** Java BigDecimal converter. For JSON, please prefer to use {@link JSONProtocol.longValue} since it maps to JSON number instead of string. */
    public static final StringMarshallable<BigDecimal> BIG_DECIMAL = new StringMarshallable<BigDecimal>() {
        @Override
        public Try<BigDecimal> tryRead(String value) {
            return Try.of(() -> new BigDecimal(value)).recover(prefixMessage("Expecting an arbitrary precision decimal number "));
        }

        @Override
        public String write(BigDecimal value) {
            return value.toString();
        }
        
        @Override
        public String toString() {
            return "big decimal";
        }
    };
    
    /** Java BigInteger converter. For JSON, please prefer to use {@link JSONProtocol.longValue} since it maps to JSON number instead of string. */
    public static final StringMarshallable<BigInteger> BIG_INTEGER = new StringMarshallable<BigInteger>() {
        @Override
        public Try<BigInteger> tryRead(String value) {
            return Try.of(() -> new BigInteger(value)).recover(prefixMessage("Expecting an arbitrary precision decimal integer "));
        }

        @Override
        public String write(BigInteger value) {
            return value.toString();
        }
        
        @Override
        public String toString() {
            return "big integer";
        }
    };
    
    /** Java UUID converter */
    public static final StringMarshallable<UUID> UUID_T = new StringMarshallable<UUID>() {
        @Override
        public Try<UUID> tryRead(String value) {
            return Try.of(() -> UUID.fromString(value)).recover(prefixMessage("Expecting a UUID "));
        }

        @Override
        public String write(UUID value) {
            return value.toString();
        }
        
        @Override
        public String toString() {
            return "uuid";
        }
    };
    
    /**
     * (un)marshals an Instant using ISO 8601 ( "2014-10-23T00:35:14.800Z" )
     */
    public static final StringMarshallable<Instant> INSTANT = new StringMarshallable<Instant>() {
        @Override
        public Try<Instant> tryRead(String value) {
            return Try.of(() -> Instant.parse(value)).recover(prefixMessage("Expecting an ISO 8601 date"));
        }

        @Override
        public String write(Instant value) {
            return value.toString(); // formats as ISO 8601
        }
        
        @Override
        public String toString() {
            return "ISO 8601 date";
        }
    };
    
    private static <T> Function<Throwable,T> prefixMessage(String msg) {
        return t -> {
            if (t instanceof IllegalArgumentException) {
                throw new IllegalArgumentException(msg + t.getMessage(), t);
            } else {
                throw (RuntimeException)t;
            }
        };
    }
}
