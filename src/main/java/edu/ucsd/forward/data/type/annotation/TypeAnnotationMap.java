/**
 * 
 */
package edu.ucsd.forward.data.type.annotation;

import java.util.LinkedHashMap;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;

/**
 * Represents a set of type annotations.
 * 
 * @author Michalis Petropoulos
 * 
 * @param <T>
 *            a type annotation
 * 
 */
public class TypeAnnotationMap<T extends TypeAnnotation> extends LinkedHashMap<SchemaPath, T>
{
    private static final long   serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(TypeAnnotationMap.class);
}
