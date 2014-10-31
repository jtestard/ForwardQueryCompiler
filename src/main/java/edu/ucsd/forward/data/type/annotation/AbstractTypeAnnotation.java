/**
 * 
 */
package edu.ucsd.forward.data.type.annotation;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.exception.Location;

/**
 * Represents an annotation of a type node in a schema tree. The annotation is optionally named and can correspond to a specific
 * location.
 * 
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractTypeAnnotation implements TypeAnnotation
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractTypeAnnotation.class);
    
    private SchemaPath          m_path;
    
    private String              m_name;
    
    private Location            m_location;
    
    /**
     * Constructor.
     * 
     * @param path
     *            the schema path to the annotated type.
     */
    protected AbstractTypeAnnotation(SchemaPath path)
    {
        this(path, null, null);
    }
    
    /**
     * Constructor.
     * 
     * @param path
     *            the schema path to the annotated type.
     * @param location
     *            a location.
     */
    protected AbstractTypeAnnotation(SchemaPath path, Location location)
    {
        this(path, null, location);
    }
    
    /**
     * Constructor.
     * 
     * @param path
     *            the schema path to the annotated type.
     * @param name
     *            the name of the annotation.
     * @param location
     *            a location.
     */
    protected AbstractTypeAnnotation(SchemaPath path, String name, Location location)
    {
        assert (path != null);
        
        m_path = path;
        
        m_name = name;
        
        m_location = location;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.type.annotation.TypeAnnotation#getSchemaPath()
     */
    @Override
    public SchemaPath getSchemaPath()
    {
        return m_path;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.type.annotation.TypeAnnotation#getName()
     */
    @Override
    public String getName()
    {
        return m_name;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.type.annotation.TypeAnnotation#setName(java.lang.String)
     */
    @Override
    public void setName(String name)
    {
        m_name = name;
        
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.type.annotation.TypeAnnotation#getLocation()
     */
    @Override
    public Location getLocation()
    {
        return m_location;
    }
    
}
