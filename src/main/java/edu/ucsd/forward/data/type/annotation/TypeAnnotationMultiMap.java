/**
 * 
 */
package edu.ucsd.forward.data.type.annotation;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;

/**
 * Represents a set of type annotations.
 * 
 * @author Hongping Lim
 * 
 * @param <T>
 *            a type annotation
 * 
 */
public class TypeAnnotationMultiMap<T extends TypeAnnotation> extends LinkedHashMap<SchemaPath, List<T>>
{
    private static final long   serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(TypeAnnotationMultiMap.class);
    
    /**
     * Adds an item to the multimap.
     * 
     * @param path
     *            key
     * @param item
     *            value
     */
    public void addToMultiMap(SchemaPath path, T item)
    {
        List<T> list;
        if (containsKey(path))
        {
            list = get(path);
        }
        else
        {
            list = new ArrayList<T>();
            put(path, list);
        }
        list.add(item);
    }
}
