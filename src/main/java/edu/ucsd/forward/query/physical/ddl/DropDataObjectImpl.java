/**
 * 
 */
package edu.ucsd.forward.query.physical.ddl;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.QueryProcessor.DataSourceAccess;
import edu.ucsd.forward.query.logical.ddl.AbstractDdlOperator;
import edu.ucsd.forward.query.logical.ddl.DropDataObject;
import edu.ucsd.forward.query.physical.AbstractOperatorImpl;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.CopyContext;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;

/**
 * Represents an implementation of the drop data object operator. This operator is executed by the mediator and drops an existing
 * data object from the specified data source.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class DropDataObjectImpl extends AbstractOperatorImpl<DropDataObject>
{
    @SuppressWarnings("unused")
    private static final Logger log     = Logger.getLogger(DropDataObjectImpl.class);
    
    private static final int    SUCCESS = 0;
    
    /**
     * Indicates whether the operation has been done.
     */
    private boolean             m_done;
    
    /**
     * Constructs an instance of a schema object definition operator implementation.
     * 
     * @param logical
     *            a logical data object ground operator.
     */
    public DropDataObjectImpl(DropDataObject logical)
    {
        super(logical);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        
        m_done = false;
    }
    
    @Override
    public Binding next() throws QueryExecutionException
    {
        if (m_done) return null;
        
        DropDataObject drop = getOperator();
        
        // Retrieve the data source access
        DataSourceAccess access = QueryProcessorFactory.getInstance().getDataSourceAccess(drop.getDataSourceName());
        
        // Delete the data object
        access.getDataSource().deleteDataObject(drop.getSchemaName(), access.getTransaction());
        
        // Drop the schema object
        access.getDataSource().dropSchemaObject(drop.getSchemaName(), access.getTransaction());
        
        // Create a binding
        Binding binding = new Binding();
        TupleValue tup_value = new TupleValue();
        tup_value.setAttribute(AbstractDdlOperator.STATUS_ATTR, new IntegerValue(SUCCESS));
        binding.addValue(new BindingValue(tup_value, true));
        m_done = true;
        
        return binding;
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        DropDataObjectImpl copy = new DropDataObjectImpl(this.getOperator());
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitDropSchemaObjectImpl(this);
    }
    
}
