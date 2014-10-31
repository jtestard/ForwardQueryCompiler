/**
 * 
 */
package edu.ucsd.forward.query.ast.dml;

import edu.ucsd.forward.query.ast.AliasedAstNode;
import edu.ucsd.forward.query.ast.ValueExpression;

/**
 * The query belongs to data manipulation language.
 * 
 * @author Yupeng
 * 
 */
public interface AliasedDmlStatement extends DmlStatement, AliasedAstNode
{
    /**
     * Gets the search condition.
     * 
     * @return the search condition.
     */
    public ValueExpression getCondition();
    
}
