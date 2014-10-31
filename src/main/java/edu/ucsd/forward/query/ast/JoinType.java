/**
 * 
 */
package edu.ucsd.forward.query.ast;

/**
 * The join types.
 * 
 * @author Yupeng
 * 
 */
public enum JoinType
{
    INNER("INNER"),

    LEFT_OUTER("LEFT OUTER"),

    RIGHT_OUTER("RIGHT OUTER"),

    FULL_OUTER("FULL OUTER"),

    CROSS("CROSS");
    
    private final String m_name;
    
    /**
     * The display name of the JOIN type.
     * 
     * @param name
     *            the display name of the JOIN type.
     */
    JoinType(String name)
    {
        m_name = name;
    }
    
    /**
     * Gets the name of the JOIN type.
     * 
     * @return the name of the JOIN type
     */
    public String getName()
    {
        return m_name;
    }
}
