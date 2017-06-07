package akka.stream.alpakka.marshal.csv;

/**
 * Represents a single eveny in a CSV stream.
 */
public abstract class CsvEvent {
    public static class Text extends CsvEvent {
        private final String text;

        private Text(String text) {
            this.text = text;
        }
        
        public String getText() {
            return text;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((text == null) ? 0 : text.hashCode());
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
            Text other = (Text) obj;
            if (text == null) {
                if (other.text != null)
                    return false;
            } else if (!text.equals(other.text))
                return false;
            return true;
        }
        
        @Override
        public String toString() {
            return "\"" + text + "\"";
        }
    }

    /**
     * @author jan
     *
     */
    public static class EndRecord extends CsvEvent {
        private EndRecord() {}
        
        @Override
        public int hashCode() {
            return 0;
        }
        
        @Override
        public boolean equals(Object obj) {
            return (obj instanceof EndRecord);
        }
        
        @Override
        public String toString() {
            return "endRecord()";
        }
    }
    
    private static final EndRecord END_RECORD = new EndRecord();
    
    public static EndRecord endRecord() {
        return END_RECORD;
    }
    
    public static class EndValue extends CsvEvent {
        private EndValue() {}
        
        @Override
        public int hashCode() {
            return 0;
        }
        
        @Override
        public boolean equals(Object obj) {
            return (obj instanceof EndValue);
        }
        
        @Override
        public String toString() {
            return "endValue()";
        }
    }
    
    private static final EndValue END_VALUE = new EndValue();
    
    public static EndValue endValue() {
        return END_VALUE;
    }
    
    public static Text text(String text) {
        return new Text(text);
    }
}
