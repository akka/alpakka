package akka.stream.alpakka.marshal.csv;

/**
 * Encapsulates the settings that make up a particular CSV variant.
 */
public class CsvSettings {
    /**
     * Describes the "official" CSV format, according to https://tools.ietf.org/html/rfc4180 , which uses
     * a double quote (") to quote fields, two double quotes ("") to represent a double qoute within a field,
     * and a semicolon (;) to separate fields.
     */
    public static final CsvSettings RFC4180 = new CsvSettings('"', "\"\"", ';');
    
    private final char quote;
    private final String escapedQuote;
    private final char separator;

    /**
     * Creates a new CsvSettings with the given values.
     */
    public CsvSettings(char quote, String escapedQuote, char separator) {
        this.quote = quote;
        this.escapedQuote = escapedQuote;
        this.separator = separator;
    }

    /**
     * Returns the character used to (optionally) quote fields, by starting and finishing the field value with this character.
     */
    public char getQuote() {
        return quote;
    }
    
    /**
     * Returns the string which is used when an actual quote (the return value of {{@link #getQuote()}) occurs in a quoted field's value.
     */
    public String getEscapedQuote() {
        return escapedQuote;
    }
    
    /**
     * Returns the separator character that goes between fields.
     */
    public char getSeparator() {
        return separator;
    }

    /**
     * Returns [s] with any escaped quotes replaced by quotes.
     */
    public String unescape(String s) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            if (s.regionMatches(i, getEscapedQuote(), 0, getEscapedQuote().length())) {
                result.append(getQuote());
                i += getEscapedQuote().length() - 1; // skip over remaining chars
            } else {
                result.append(s.charAt(i));
            }
        }
        return result.toString();
    }

    /**
     * Returns [s] with any quote characters replaced by escaped quotes.
     */
    public String escape(String s) {
        return s.replace(String.valueOf(quote), escapedQuote);
    }
}
