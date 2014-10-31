/**
 * 
 */
package edu.ucsd.forward.query.ast.dml;

import edu.ucsd.forward.query.ast.AttributeReference;
import edu.ucsd.forward.query.ast.QueryStatement;

/**
 * The query belongs to data manipulation language.
 * 
 * @author Yupeng
 * 
 */
public interface DmlStatement extends QueryStatement
{
    /**
     * Gets the reference to the target type.
     * 
     * @return the reference to the target type.
     */
    public AttributeReference getTarget();
    
}
