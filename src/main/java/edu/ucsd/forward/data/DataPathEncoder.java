/**
 * 
 */
package edu.ucsd.forward.data;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * Percent encodes and decodes strings. The following characters will be percent escaped : ' ', '.', '#', ',', '\', '%'
 * 
 * FIXME : LightWeightAPI : Use data path encoder to encode keys.
 * 
 * @author Erick Zamora
 */
public class DataPathEncoder
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DataPathEncoder.class);
    
    /**
     * Private Constructor.
     */
    private DataPathEncoder()
    {
    }
    
    /**
     * Percent encodes a string.
     * 
     * @param str
     *            The string to encode.
     * @return the percent encoded string.
     */
    public static String encode(String str)
    {
        StringBuilder builder = new StringBuilder();
        int i = 0;
        while (i < str.length())
        {
            char c = str.charAt(i);
            switch (c)
            {
                case ' ':
                case '.':
                case '#':
                case ',':
                case '\\':
                case '%':
                    builder.append("%" + Integer.toHexString((int) c).toUpperCase());
                    break;
                default:
                    builder.append(c);
            }
            i++;
        }
        return builder.toString();
    }
    
    /**
     * Decodes a string that was encoded by DataPathEncoder.encode().
     * 
     * @param str
     *            the string to decode.
     * @return the decoded string.
     */
    public static String decode(String str)
    {
        StringBuilder decoded = new StringBuilder();
        int i = 0;
        while (i < str.length())
        {
            char char_i = str.charAt(i);
            switch (char_i)
            {
                case '%':
                    int char_int = Integer.parseInt(str.substring(i + 1, i + 3), 16);
                    decoded.append((char) char_int);
                    i += 2;
                    break;
                default:
                    decoded.append(char_i);
            }
            i++;
        }
        
        return decoded.toString();
    }
}
