package akka.stream.alpakka.marshal.json;

import static akka.stream.alpakka.marshal.ReadProtocol.none;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import akka.stream.alpakka.marshal.json.JSONEvent.Value;
import akka.stream.alpakka.marshal.Protocol;
import akka.stream.alpakka.marshal.Reader;
import akka.stream.alpakka.marshal.Writer;

import javaslang.Function1;
import javaslang.collection.Vector;
import javaslang.control.Try;

public class ValueProtocol<T> implements Protocol<JSONEvent, T> {
    // Only numeric and boolean types are defined here, since they have to marshal to numbers and booleans in JSON.
    // For everything that marshals to strings, use stringValue.as(...)
    
    /** A Java integer represented as a JSON number (on reading, JSON string is also allowed) */
    public static final ValueProtocol<Integer> INTEGER = of("signed 32-bit integer",
        evt -> Integer.parseInt(evt.getValueAsString()),
        i -> new JSONEvent.NumericValue(String.valueOf(i)));
    
    /** A Java long represented as a JSON number (on reading, JSON string is also allowed) */
    public static final ValueProtocol<Long> LONG = of("signed 64-bit integer",
        evt -> Long.parseLong(evt.getValueAsString()),
        l -> new JSONEvent.NumericValue(String.valueOf(l)));

    /** A Java big decimal represented as a JSON number (on reading, JSON string is also allowed) */
    public static final ValueProtocol<BigDecimal> BIGDECIMAL = of("arbitrary precision decimal",
        evt -> new BigDecimal(evt.getValueAsString()),
        d -> new JSONEvent.NumericValue(String.valueOf(d)));
    
    /** A Java big integer represented as a JSON number (on reading, JSON string is also allowed) */
    public static final ValueProtocol<BigInteger> BIGINTEGER = of("arbitrary precision integer",
        evt -> new BigInteger(evt.getValueAsString()),
        d -> new JSONEvent.NumericValue(String.valueOf(d)));
    
    /** A Java boolean represented a JSON boolean (on reading, a JSON string of "true" or "false" is also allowed) */
    public static final ValueProtocol<Boolean> BOOLEAN = of("boolean",
        v -> v.getValueAsString().equals("true"),
        b -> b ? JSONEvent.TRUE : JSONEvent.FALSE);
    
    /** A Java String. Internal implementation, @see {@link StringValueProtocol} */
    static final ValueProtocol<String> STRING = of("string",
        evt -> evt.getValueAsString(),
        s -> new JSONEvent.StringValue(s));
    
    private static final Logger log = LoggerFactory.getLogger(ValueProtocol.class);
    
    private final Function1<Value, T> tryRead;
    private final Function1<T,Value> write;
    private final String description;
    
    public static <T> ValueProtocol<T> of(String description, Function1<Value, T> tryRead, Function1<T, Value> write) {
        return new ValueProtocol<>(description, tryRead, write);
    }
    
    protected ValueProtocol(String description, Function1<Value, T> tryRead, Function1<T, Value> write) {
        this.description = description;
        this.tryRead = tryRead;
        this.write = write;
    }

    @Override
    public Reader<JSONEvent, T> reader() {
        return new Reader<JSONEvent, T>() {
            private int level = 0;
            
            @Override
            public Try<T> reset() {
                level = 0;
                return none();
            }

            @Override
            public Try<T> apply(JSONEvent evt) {
                if (evt == JSONEvent.START_OBJECT || evt == JSONEvent.START_ARRAY) {
                    level++;
                } else if (evt == JSONEvent.END_OBJECT || evt == JSONEvent.END_ARRAY) {
                    level--;
                }
                
                if (level == 0 && evt instanceof JSONEvent.Value) {
                    Try<T> result = Try.of(() -> {
                        try {
                            return tryRead.apply(JSONEvent.Value.class.cast(evt));
                        } catch (IllegalArgumentException x) {
                            String msg = (x.getMessage() == null) ? "" : ": " + x.getMessage();
                            throw new IllegalArgumentException ("Expecting " + description + msg);
                        }
                    });
                    
                    log.info("Read {}", result);
                    return result;
                } else {
                    return none();
                }
            }

        };
    }
    
    @Override
    public Class<? extends JSONEvent> getEventType() {
        return JSONEvent.class;
    }

    @Override
    public Writer<JSONEvent, T> writer() {
        return Writer.of(value -> Vector.of(write.apply(value)));
    }

    @Override
    public String toString() {
        return description;
    }
}
