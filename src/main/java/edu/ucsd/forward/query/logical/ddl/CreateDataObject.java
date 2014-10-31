/**
 * 
 */
package edu.ucsd.forward.query.logical.ddl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * The operator that creates a schema object for a data object in the specified data source.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class CreateDataObject extends AbstractDdlOperator
{
    @SuppressWarnings("unused")
    private static final Logger          log = Logger.getLogger(CreateDataObject.class);
    
    private String                       m_schema_name;
    
    private SchemaTree                   m_schema_tree;
    
    private String                       m_data_source_name;
    
    private Map<SchemaPath, LogicalPlan> m_default_plans;
    
    /**
     * The bound parameters.
     */
    private List<Parameter>              m_bound_params;
    
    /**
     * Constructs the operator with the schema name, schema tree definition and data source name.
     * 
     * @param schema_name
     *            the name of the schema
     * @param schema_tree
     *            the schema tree of the schema object
     * @param data_source_name
     *            the name of the data source where the schema object to be created.
     */
    public CreateDataObject(String schema_name, SchemaTree schema_tree, String data_source_name)
    {
        super();
        
        assert schema_name != null;
        assert schema_tree != null;
        assert data_source_name != null;
        
        m_schema_name = schema_name;
        m_schema_tree = schema_tree;
        m_data_source_name = data_source_name;
        
        m_default_plans = new LinkedHashMap<SchemaPath, LogicalPlan>();
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private CreateDataObject()
    {
        
    }
    
    /**
     * Adds the logical plan for evaluating the default value of a given type.
     * 
     * @param path
     *            the scheme path to navigate to the type
     * @param plan
     *            the logical plan
     */
    public void addDefaultPlan(SchemaPath path, LogicalPlan plan)
    {
        assert path != null;
        assert plan != null;
        m_default_plans.put(path, plan);
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
     * Gets the default logical plan of a given schema path.
     * 
     * @param path
     *            the schema path
     * @return the default logical plan
     */
    public LogicalPlan getDefaultPlan(SchemaPath path)
    {
        return m_default_plans.get(path);
    }
    
    /**
     * Gets the schema name.
     * 
     * @return the schema name
     */
    public String getSchemaName()
    {
        return m_schema_name;
    }
    
    /**
     * Gets the name of the data source.
     * 
     * @return the name of the data source.
     */
    public String getDataSourceName()
    {
        return m_data_source_name;
    }
    
    /**
     * Gets the schema tree.
     * 
     * @return the schema tree.
     */
    public SchemaTree getSchemaTree()
    {
        return m_schema_tree;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitCreateSchemaObject(this);
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        // Set the bound parameters
        this.setBoundParametersUsed();
        
        // Validate the bound parameters
        for (Parameter param : this.getBoundParametersUsed())
            param.getTerm().inferType(this.getChildren());
        
        super.updateOutputInfo();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        // From the parameters of the logical plans, keep only the parameters that do not appear in the input info.
        List<Parameter> free_params = new ArrayList<Parameter>();
        for (LogicalPlan plan : m_default_plans.values())
        {
            
            for (Parameter free_param : plan.getFreeParametersUsed())
            {
                if (!this.getInputVariables().contains(free_param.getVariable()))
                {
                    free_params.add(free_param);
                }
            }
        }
        
        return free_params;
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        // Needed during parsing of an existing plan
        if (m_bound_params == null)
        {
            this.setBoundParametersUsed();
        }
        
        return m_bound_params;
    }
    
    /**
     * Sets the bounding parameters used by the operator.
     */
    private void setBoundParametersUsed()
    {
        m_bound_params = new ArrayList<Parameter>();
        for (LogicalPlan plan : m_default_plans.values())
            m_bound_params.addAll(plan.getFreeParametersUsed());
        m_bound_params.removeAll(this.getFreeParametersUsed());
    }
    
    @Override
    public List<LogicalPlan> getLogicalPlansUsed()
    {
        List<LogicalPlan> plans = new ArrayList<LogicalPlan>();
        plans.addAll(m_default_plans.values());
        
        return plans;
    }
    
    @Override
    public Operator copy()
    {
        CreateDataObject copy = new CreateDataObject(m_schema_name, m_schema_tree, m_data_source_name);
        super.copy(copy);
        
        return copy;
    }
    
}
