package akka.stream.alpakka.marshal.json;

public abstract class JSONEvent {
    public static final class StartObject extends JSONEvent { 
        private StartObject() {}
        @Override
        public String toString() {
            return "{";
        }
    }
    public static final StartObject START_OBJECT = new StartObject();
    
    public static final class EndObject extends JSONEvent { 
        private EndObject() {}
        @Override
        public String toString() {
            return "}";
        }
    }
    public static final EndObject END_OBJECT = new EndObject();
    
    public static final class StartArray extends JSONEvent { 
        private StartArray() {}
        @Override
        public String toString() {
            return "[";
        }
    }
    public static final StartArray START_ARRAY = new StartArray();
    
    public static final class EndArray extends JSONEvent { 
        private EndArray() {} 
        @Override
        public String toString() {
            return "]";
        }
    }
    public static final EndArray END_ARRAY = new EndArray();
    
    public static final class FieldName extends JSONEvent {
        private final String name;

        public FieldName(String name) {
            this.name = name;
        }
        
        public String getName() {
            return name;
        }

        public static boolean is(JSONEvent evt, String fieldName) {
            return evt instanceof FieldName && ((FieldName)evt).name.equals(fieldName);
        }
        
        @Override
        public String toString() {
            return name + ": ";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            FieldName other = (FieldName) obj;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            return true;
        }
    }
    
    public static abstract class Value extends JSONEvent {
        public abstract String getValueAsString();
        
        @Override
        public String toString() {
            return getValueAsString();
        }
    }
    
    public static final class True extends Value { 
        private True() {}
        @Override
        public String getValueAsString() {
            return "true";
        }
    }
    public static final True TRUE = new True();
    
    public static final class False extends Value { 
        private False() {}
        @Override
        public String getValueAsString() {
            return "false";
        }
    }
    public static final False FALSE = new False();
    
    public static final class Null extends Value { 
        private Null() {}
        @Override
        public String getValueAsString() {
            return "null";
        }
    }
    public static final Null NULL = new Null();
    
    public static final class StringValue extends Value {
        private final String value;

        public StringValue(String value) {
            this.value = value;
        }

        @Override
        public String getValueAsString() {
            return value;
        }
        
        @Override
        public String toString() {
            return "\"" + value + "\"";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((value == null) ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            StringValue other = (StringValue) obj;
            if (value == null) {
                if (other.value != null)
                    return false;
            } else if (!value.equals(other.value))
                return false;
            return true;
        }
    }
    
    public static final class NumericValue extends Value {
        private final String value;

        public NumericValue(String value) {
            // TODO validate this when writing own parser, and validate when generating
            this.value = value;
        }

        @Override
        public String getValueAsString() {
            return value;
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((value == null) ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            NumericValue other = (NumericValue) obj;
            if (value == null) {
                if (other.value != null)
                    return false;
            } else if (!value.equals(other.value))
                return false;
            return true;
        }
    }
}