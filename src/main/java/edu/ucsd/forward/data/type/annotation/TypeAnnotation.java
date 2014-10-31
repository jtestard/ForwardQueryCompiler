/**
 * 
 */
package edu.ucsd.forward.data.type.annotation;

import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.exception.Location;

/**
 * Represents an annotation of a type node in a schema tree. The annotation is optionally named and can correspond to a specific
 * location.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface TypeAnnotation
{
    /**
     * Returns the schema path leading to the annotated type.
     * 
     * @return the schema path to the annotated type.
     */
    public SchemaPath getSchemaPath();
    
    /**
     * Returns the name of the annotation.
     * 
     * @return the name of the annotation.
     */
    public String getName();
    
    /**
     * Sets the name of the annotation.
     * 
     * @param name
     *            the name of the annotation.
     */
    public void setName(String name);
    
    /**
     * Returns the location of the annotation.
     * 
     * @return the location.
     */
    public Location getLocation();
    
}
