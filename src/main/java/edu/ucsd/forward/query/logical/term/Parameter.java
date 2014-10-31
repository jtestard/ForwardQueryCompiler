/**
 * 
 */
package edu.ucsd.forward.query.logical.term;

import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.source.SchemaObjectHandle;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;

/**
 * Represents a parameter in a logical plan.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class Parameter extends AbstractTerm
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(Parameter.class);
    
    /**
     * The unique identifier of the parameter.
     */
    private String              m_id;
    
    /**
     * The variable of the parameter.
     */
    private Term                m_term;
    
    /**
     * An enumeration of parameter instantiation methods.
     */
    public enum InstantiationMethod
    {
        // The parameter instantiation will appear as a term
        ASSIGN,
        // The parameter instantiation will appear as a temporary data object
        COPY;
    }
    
    /**
     * The parameter instantiation method.
     */
    private InstantiationMethod m_instantiation_method;
    
    /**
     * The schema object handle in case of copy instantiation method.
     */
    private SchemaObjectHandle  m_handle;
    
    /**
     * Initializes an instance of the parameter for the ASSIGN instantiation method. The parameter can only be a relative variable
     * or a query path starting from a relative variable. The constructor assumes that the type of the given term is set or
     * inferred.
     * 
     * @param term
     *            the term of the parameter.
     */
    public Parameter(Term term)
    {
        m_id = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.PARAMETER_GENERATOR);
        
        assert (term != null);
        m_term = term;
        
        assert (term.getType() != null);
        this.setType(m_term.getType());
        
        m_instantiation_method = InstantiationMethod.ASSIGN;
    }
    
    /**
     * Private constructor.
     */
    @SuppressWarnings("unused")
    private Parameter()
    {
        
    }    
    
    
    /**
     * Initializes an instance of the parameter for the ASSIGN instantiation method. The parameter can only be a relative variable
     * or a query path starting from a relative variable. The constructor assumes that the type of the given term is set or
     * inferred.
     * 
     * @param term
     *            the term of the parameter.
     * @param name 
     *            the name of the parameter.
     */
    public Parameter(Term term, String name)
    {
        m_id = name;
        
        assert (term != null);
        m_term = term;
        
        assert (term.getType() != null);
        this.setType(m_term.getType());
        
        m_instantiation_method = InstantiationMethod.ASSIGN;
    }    
    

    /**
     * Initializes an instance of the parameter for the COPY instantiation method. The parameter can only be a relative variable or
     * a query path starting from a relative variable. The constructor assumes that the type of the given term is set or inferred.
     * 
     * @param term
     *            the term of the parameter.
     * @param data_source
     *            the data source the parameter value will be copied to.
     * @param schema_object
     *            the schema object the parameter value will be copied to.
     */
    public Parameter(Term term, String data_source, String schema_object)
    {
        this(term);
        
        m_handle = new SchemaObjectHandle(data_source, schema_object);
        
        m_instantiation_method = InstantiationMethod.COPY;
    }
    
    /**
     * Returns the unique identifier of the parameter.
     * 
     * @return the unique identifier of the parameter.
     */
    public String getId()
    {
        return m_id;
    }
    
    /**
     * Returns the term of the parameter.
     * 
     * @return the term of the parameter.
     */
    public Term getTerm()
    {
        return m_term;
    }
    
    /**
     * Returns the relative variable of the parameter. If the parameter's term is a relative variable, return the variable itset. If
     * the term is a query path, return the starting relative variable of the term.
     * 
     * @return the relative variable of the parameter.
     */
    public RelativeVariable getVariable()
    {
        RelativeVariable rel_var;
        if (m_term instanceof RelativeVariable)
        {
            rel_var = (RelativeVariable) m_term;
        }
        else
        {
            assert (m_term instanceof QueryPath);
            QueryPath qp = (QueryPath) m_term;
            
            assert (qp.getTerm() instanceof RelativeVariable);
            
            rel_var = (RelativeVariable) qp.getTerm();
        }
        
        return rel_var;
    }
    
    /**
     * Returns the name of the data source in case of copy instantiation method.
     * 
     * @return the name of the data source in case of copy instantiation method.
     */
    public String getDataSourceName()
    {
        return m_handle.getDataSourceName();
    }
    
    /**
     * Returns the name of the schema object in case of copy instantiation method.
     * 
     * @return the name of the schema object in case of copy instantiation method.
     */
    public String getSchemaObjectName()
    {
        return m_handle.getSchemaObjectName();
    }
    
    @Override
    public Type inferType(List<Operator> operators) throws QueryCompilationException
    {
        return this.getType();
    }
    
    /**
     * Returns the instantiation method of the parameter.
     * 
     * @return the instantiation method of the parameter.
     */
    public InstantiationMethod getInstantiationMethod()
    {
        return m_instantiation_method;
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        // The parameter does not use any variables since it's never evaluated, but rather instantiated. Hence, its
        // variable should not influence plan distribution, which utilizes this method.
        return Collections.emptyList();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        return Collections.<Parameter> singletonList(this);
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        return true;
    }
    
    @Override
    public String getDefaultProjectAlias()
    {
        return m_term.getDefaultProjectAlias();
    }
    
    @Override
    public String toExplainString()
    {
        String str = m_term.toExplainString();
        
        return str;
    }
    
    @Override
    public String toString()
    {
        return this.toExplainString();
    }
    
    
    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof Parameter)) return false;
        
        return m_term.equals(((Parameter) other).m_term);
    }
    
    @Override
    public int hashCode()
    {
        return m_term.hashCode();
    }
    
    @Override
    public Term copy()
    {
        Term copy_term = m_term.copy();
        if(copy_term instanceof RelativeVariable)
        {
            ((RelativeVariable) copy_term).setType(m_term.getType());
        }
        else if(copy_term instanceof QueryPath)
        {
            ((QueryPath) copy_term).setType(m_term.getType());
        }
        
        Parameter copy = new Parameter(copy_term);
        super.copy(copy);
        
        copy.m_instantiation_method = m_instantiation_method;
        copy.m_handle = m_handle;
        
        return copy;
    }
}
