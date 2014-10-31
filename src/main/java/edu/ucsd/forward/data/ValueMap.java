/**
 * 
 */
package edu.ucsd.forward.data;

import java.util.IdentityHashMap;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.Value;

/**
 * Map between two data nodes. Reference-equality is used for comparing data.
 * 
 * @author Kian Win
 * 
 */
public class ValueMap extends IdentityHashMap<Value, Value>
{
    
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(ValueMap.class);
    
    private static final long   serialVersionUID = 1L;
}
