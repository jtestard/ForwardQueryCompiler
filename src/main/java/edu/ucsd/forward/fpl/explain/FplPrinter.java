/**
 * 
 */
package edu.ucsd.forward.fpl.explain;

/**
 * Generates a function in action language.
 * 
 * @author Yupeng
 * 
 */
public interface FplPrinter
{
    public static final String COMMA       = ",";
    
    public static final String NL          = "\n";
    
    public static final String LEFT_PAREN  = "(";
    
    public static final String RIGHT_PAREN = ")";
    
    /**
     * Converts an action construct to a string for debugging purpose.
     * 
     * @param tabs
     *            number of leading tabs.
     * @return the string representation of action constructs.
     */
    public void toActionString(StringBuilder sb, int tabs);
}
