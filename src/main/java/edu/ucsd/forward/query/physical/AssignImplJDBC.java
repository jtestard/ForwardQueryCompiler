/**
 * 
 */
package edu.ucsd.forward.query.physical;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.SchemaObject.SchemaObjectScope;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSource;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.EagerQueryResult;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessor.DataSourceAccess;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * The implementation of the Assign operator whose temporary data object are stored in the database.
 * 
 * @author Vicky Papavasileiou
 * 
 */
public class AssignImplJDBC extends AbstractAssignImpl
{
    private static final long   serialVersionUID = 1L;
    
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(AssignImplJDBC.class);
        
    /**
     * The query result of evaluating the assign plan.
     */
    private EagerQueryResult    m_result;
    
    private boolean             m_done;
    
    /**
     * Constructor.
     * 
     * @param assign
     *            the logical assign operator.
     */
    public AssignImplJDBC(Assign assign)
    {
        super(assign);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        m_result = null;
        m_done = false;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        super.close();
        
        if(m_result != null)
            m_result.close();
    }
    
    /**
     * Gets the next binding for the referencing scan operator.
     * 
     * @param reference
     *            the referencing scan operator.
     * @return the next binding that the referencing scan operator requests.
     * @throws QueryExecutionException
     *             if anything goes wrong calling assign's next method.
     * @throws SuspensionException
     *             when the execution suspenses
     */
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        if (!m_done)
        {
            QueryProcessor qp = QueryProcessorFactory.getInstance();
            
            // Retrieve the data source access
            DataSourceAccess access = qp.getDataSourceAccess(getOperator().getExecutionDataSourceName());
            String name = this.getOperator().getTarget();
            Type type = this.getAssignPlan().getLogicalPlan().getOutputType();
            assert (type instanceof CollectionType);
            Size estimate = this.getOperator().getCardinalityEstimate();
            
            // Do not create a temporary table if the type is a collection of tuples with no attributes.
            // This can happen if a selection is pushed all the way down, right above a Ground operator.
            if (((CollectionType) type).getTupleType().getSize() > 0)
            {
                if (m_result == null && !access.getDataSource().hasSchemaObject(name, access.getTransaction()))
                {
                    SchemaTree schema = new SchemaTree(type);
                    access.getDataSource().createSchemaObject(name, SchemaObjectScope.TEMPORARY, schema, estimate,
                                                              access.getTransaction());
                }
                
                // Eagerly execute the copy physical plan
                QueryProcessor processor = QueryProcessorFactory.getInstance();
                m_result = processor.createEagerQueryResult(this.getAssignPlan(), processor.getUnifiedApplicationState());
                Value query_result = m_result.getValueWithSuspension();
                
                // Build a data tree with the result of the copy physical plan
                DataTree data_tree = new DataTree(query_result);
                
                // Set the data object
                ((JdbcDataSource) access.getDataSource()).setDataObjectUsingCopy(name, data_tree, access.getTransaction());
            }
            m_done = true;
        }
        return null;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitAssignImplJDBC(this);
    }
    
    @Override
    public OperatorImpl copy(CopyContext context)
    {
        AssignImplJDBC copy = new AssignImplJDBC(getOperator());
        copy.setAssignPlan(this.getAssignPlan().copy(context));
        // Add the copy in context
        context.addAssignTarget(getOperator().getTarget(), copy);
        return copy;
    }
    
    @Override
    protected void addReference(ScanImplReferencing reference)
    {
        throw new UnsupportedOperationException();        
    }

    @Override
    protected Binding getNext(ScanImplReferencing reference) throws QueryExecutionException, SuspensionException
    {
        throw new UnsupportedOperationException();
    }
    
}
