/**
 * 
 */
package edu.ucsd.forward.query.physical.dml;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.physical.AbstractUnaryOperatorImpl;
import edu.ucsd.forward.query.physical.OperatorImpl;

/**
 * An abstract implementation of the delete operator implementation interface.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractDeleteImpl extends AbstractUnaryOperatorImpl<Delete> implements DeleteImpl
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractDeleteImpl.class);
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical delete operator.
     * @param child
     *            the single child operator implementation.
     */
    public AbstractDeleteImpl(Delete logical, OperatorImpl child)
    {
        super(logical);
        
        assert (child != null);
        addChild(child);
    }
    
}
