package edu.ucsd.forward.query.physical;

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
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.logical.Copy;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.Parameter.InstantiationMethod;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;
import edu.ucsd.forward.query.suspension.SuspensionException;

/**
 * Represents an implementation of the copy interface that retrieves the query result of the nested physical plan in memory and then
 * copies it to the target data source.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 * 
 */
public class CopyImplInMemory extends AbstractCopyImpl
{
    private boolean          m_done;
    
    /**
     * The result holds the plan result.
     */
    private EagerQueryResult m_query_result;
    
    /**
     * Creates an instance of the operator implementation.
     * 
     * @param logical
     *            a logical copy operator.
     */
    public CopyImplInMemory(Copy logical)
    {
        super(logical);
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        super.open();
        m_query_result = null;
        m_done = false;
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        if (!m_done)
        {
            QueryProcessor qp = QueryProcessorFactory.getInstance();
            
            // Retrieve the data source access
            DataSourceAccess access = qp.getDataSourceAccess(getOperator().getTargetDataSourceName());
            String name = this.getOperator().getTargetSchemaObjectName();
            Type type = this.getOperator().getCopyPlan().getOutputType();
            assert (type instanceof CollectionType);
            Size estimate = this.getOperator().getCardinalityEstimate();
            
            // Get the Parameter destination of the Copy operator
            assert (this.getOperator().getParent() instanceof SendPlan);
            Parameter param = null;
            SendPlan parent = (SendPlan) this.getOperator().getParent();
            for (Parameter p : parent.getBoundParametersUsed())
            {
                if (((Variable) p.getTerm()).getName().equals(name))
                {
                    param = p;
                }
            }
            assert (param != null);
            
            if (param.getInstantiationMethod().equals(InstantiationMethod.COPY))
            {
                
                // Do not create a temporary table if the type is a collection of tuples with no attributes.
                // This can happen if a selection is pushed all the way down, right above a Ground operator.
                if (((CollectionType) type).getTupleType().getSize() > 0)
                {
                    if (m_query_result == null && !access.getDataSource().hasSchemaObject(name, access.getTransaction()))
                    {
                        SchemaTree schema = new SchemaTree(type);
                        access.getDataSource().createSchemaObject(name, SchemaObjectScope.TEMPORARY, schema, estimate,
                                                                  access.getTransaction());
                    }
                    
                    // Eagerly execute the copy physical plan
                    QueryProcessor processor = QueryProcessorFactory.getInstance();
                    m_query_result = processor.createEagerQueryResult(this.getCopyPlan(), processor.getUnifiedApplicationState());
                    Value query_result = m_query_result.getValueWithSuspension();
                    m_query_result = null;
                    
                    // Build a data tree with the result of the copy physical plan
                    DataTree data_tree = new DataTree(query_result);
                    
                    // Set the data object
                    ((JdbcDataSource) access.getDataSource()).setDataObjectUsingCopy(name, data_tree, access.getTransaction());
                    
                }
            }
            else if (param.getInstantiationMethod().equals(InstantiationMethod.ASSIGN))
            {
                // Eagerly execute the copy physical plan
                QueryProcessor processor = QueryProcessorFactory.getInstance();
                m_query_result = processor.createEagerQueryResult(this.getCopyPlan(), processor.getUnifiedApplicationState());
                Value query_result = m_query_result.getValueWithSuspension();
                m_query_result = null;
                qp.getParameterInstantiations().setInstantiation(param, new BindingValue(query_result, true));
            }
            
            m_done = true;
        }
        
        return null;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (this.getState() == State.OPEN)
        {
            // Retrieve the data source access
            DataSourceAccess access = QueryProcessorFactory.getInstance().getDataSourceAccess(getOperator().getTargetDataSourceName());
            
            // Just truncate the transient data object.
            // The corresponding transient schema objects will be dropped when the query processor cleans up.
            String name = this.getOperator().getTargetSchemaObjectName();
            // The transient data object might have been dropped by the JDBC data source in case of exception
            if (access.getDataSource().hasSchemaObject(name, access.getTransaction()))
            {
                access.getDataSource().deleteDataObject(name, access.getTransaction());
            }
        }
        
        super.close();
    }
    
    @Override
    public CopyImplInMemory copy(CopyContext context)
    {
        CopyImplInMemory copy = new CopyImplInMemory(this.getOperator());
        
        copy.setCopyPlan(this.getCopyPlan().copy(context));
        
        return copy;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitCopyImplInMemory(this);
    }
    
}
