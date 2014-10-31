/**
 * 
 */
package edu.ucsd.forward.query.physical.ddl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.source.SchemaObject.SchemaObjectScope;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSource;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.EagerQueryResult;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.QueryProcessor.DataSourceAccess;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.logical.ddl.AbstractDdlOperator;
import edu.ucsd.forward.query.logical.ddl.CreateDataObject;
import edu.ucsd.forward.query.physical.AbstractOperatorImpl;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.CopyContext;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.physical.visitor.OperatorImplVisitor;

/**
 * Represents an implementation of the create data object operator. This operator is executed by the mediator and creates a schema
 * object for a data object in the specified data source.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class CreateDataObjectImpl extends AbstractOperatorImpl<CreateDataObject>
{
    @SuppressWarnings("unused")
    private static final Logger           log     = Logger.getLogger(CreateDataObjectImpl.class);
    
    private static final int              SUCCESS = 0;
    
    private Map<SchemaPath, PhysicalPlan> m_default_plans;
    
    /**
     * Indicates whether the operation has been done.
     */
    private boolean                       m_done;
    
    /**
     * Constructs an instance of a schema object definition operator implementation.
     * 
     * @param logical
     *            a logical data object creation operator.
     */
    public CreateDataObjectImpl(CreateDataObject logical)
    {
        super(logical);
        m_default_plans = new LinkedHashMap<SchemaPath, PhysicalPlan>();
    }
    
    /**
     * Gets the set of schema paths to types for default value evaluation.
     * 
     * @return the set of schema paths to types for default value evaluation.
     */
    public Set<SchemaPath> getTypesToSetDefault()
    {
        return m_default_plans.keySet();
    }
    
    /**
     * Gets the default physical plan of a given schema path.
     * 
     * @param path
     *            the schema path
     * @return the default logical plan
     */
    public PhysicalPlan getDefaultPlan(SchemaPath path)
    {
        return m_default_plans.get(path);
    }
    
    /**
     * Adds the physical plan for evaluating the default value of a given type.
     * 
     * @param path
     *            the scheme path to navigate to the type
     * @param plan
     *            the physical plan
     */
    public void addDefaultPlan(SchemaPath path, PhysicalPlan plan)
    {
        assert path != null;
        assert plan != null;
        m_default_plans.put(path, plan);
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
        
        CreateDataObject definition = getOperator();
        
        SchemaTree schema_tree = definition.getSchemaTree();
        
        // Evaluate the default plans
        // FIXME: Do not support suspension and resume operation
        for (SchemaPath path : m_default_plans.keySet())
        {
            // Fully execute the option physical plan
            QueryProcessor qp = QueryProcessorFactory.getInstance();
            EagerQueryResult query_result = qp.createEagerQueryResult(m_default_plans.get(path), qp.getUnifiedApplicationState());
            Value default_value = query_result.getValue();
            // Set the default value to the type
            Type target_type = path.find(schema_tree);
            target_type.setDefaultValue(default_value);
        }
        
        // Create the schema object
        
        // Retrieve the data source access
        DataSourceAccess access = QueryProcessorFactory.getInstance().getDataSourceAccess(definition.getDataSourceName());
        
        Size estimate = (access.getDataSource() instanceof JdbcDataSource) ? Size.LARGE : Size.SMALL;
        
        // Create the schema object
        access.getDataSource().createSchemaObject(definition.getSchemaName(), SchemaObjectScope.PERSISTENT, schema_tree, estimate,
                                                  access.getTransaction());
        
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
        CreateDataObjectImpl copy = new CreateDataObjectImpl(this.getOperator());
        for (SchemaPath path : m_default_plans.keySet())
        {
            copy.addDefaultPlan(path, m_default_plans.get(path).copy(context));
        }
        return copy;
    }
    
    @Override
    public List<PhysicalPlan> getPhysicalPlansUsed()
    {
        List<PhysicalPlan> plans = new ArrayList<PhysicalPlan>();
        plans.addAll(m_default_plans.values());
        
        return plans;
    }
    
    @Override
    public void accept(OperatorImplVisitor visitor)
    {
        visitor.visitCreateDataObjectImpl(this);
    }
    
}
