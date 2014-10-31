/**
 * 
 */
package edu.ucsd.forward.query.logical.term;

import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.exception.Location;

/**
 * Represents an abstract implementation of the term interface.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public abstract class AbstractTerm implements Term
{
    private Type     m_type = null;
    
    private Location m_location;
    
    @Override
    public Type getType()
    {
        return m_type;
    }
    
    /**
     * Default constructor.
     */
    public AbstractTerm()
    {
        
    }
    
    /**
     * Sets the type of the term.
     * 
     * @param type
     *            the type of the term.
     */
    public void setType(Type type)
    {
        assert (type != null);
        
        m_type = type;
    }
    
    @Override
    public Location getLocation()
    {
        return m_location;
    }
    
    @Override
    public void setLocation(Location location)
    {
        m_location = location;
    }
    
    /**
     * Copies common attributes to a term copy.
     * 
     * @param copy
     *            a term copy.
     */
    protected void copy(AbstractTerm copy)
    {
        copy.setType(m_type);
        copy.setLocation(m_location);
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.query.logical.term.Term#copyWithoutType()
     */
    @Override
    public Term copyWithoutType()
    {
        throw new UnsupportedOperationException();
    }
}
