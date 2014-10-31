/**
 * 
 */
package edu.ucsd.forward.data.type;

import java.util.IdentityHashMap;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * Map between two type nodes. Reference-equality is used for comparing types.
 * 
 * @author Kian Win
 * 
 */
public class TypeMap extends IdentityHashMap<Type, Type>
{

    @SuppressWarnings("unused")
    private static final Logger         log = Logger.getLogger(TypeMap.class);
    
    private static final long serialVersionUID = 1L;
}
