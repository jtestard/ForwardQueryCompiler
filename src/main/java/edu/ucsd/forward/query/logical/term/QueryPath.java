/**
 * 
 */
package edu.ucsd.forward.query.logical.term;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.StringUtil;
import edu.ucsd.app2you.util.identity.DeepEquality;
import edu.ucsd.app2you.util.identity.EqualUtil;
import edu.ucsd.app2you.util.identity.HashCodeUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.JsonType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.util.tree.TreePath.PathMode;

/**
 * The query path consists of a variable and a list of attribute names (path steps). The variable serves as the starting point of
 * the query path. An intermediate step of the query path cannot refer to a collection type.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class QueryPath extends AbstractTerm implements DeepEquality
{
    @SuppressWarnings("unused")
    private static final Logger log                    = Logger.getLogger(QueryPath.class);
    
    public static final String  PATH_SEPARATOR         = ".";
    
    public static final String  PATH_SEPARATOR_ESCAPED = "\\.";
    
    /**
     * The starting point of the query path.
     */
    private Term                m_term;
    
    private List<String>        m_path_steps;
    
    // FIXME unmodifiable collection is not GWT serializable
    // private List<String> m_path_steps_unmodifiable;
    
    /**
     * Constructor for a query path with a variable and a list of attribute names (path steps).
     * 
     * @param term
     *            the term (starting point) of the query path.
     * @param path_steps
     *            the path steps.
     */
    public QueryPath(Term term, List<String> path_steps)
    {
        assert (path_steps != null);
        assert (term != null);
        // assert (path_steps.size() > 0);
        
        m_term = term;
        m_path_steps = new ArrayList<String>(path_steps);
        // m_path_steps_unmodifiable = Collections.unmodifiableList(m_path_steps);
    }
    
    /**
     * Private constructor.
     */
    @SuppressWarnings("unused")
    private QueryPath()
    {
        
    }
    
    /**
     * Gets the term (starting point) of the query path.
     * 
     * @return the term of the query path.
     */
    public Term getTerm()
    {
        return m_term;
    }
    
    /**
     * Returns the unmodifiable path steps.
     * 
     * @return the path steps.
     */
    public List<String> getPathSteps()
    {
        return Collections.unmodifiableList(m_path_steps);
    }
    
    /**
     * Returns the last path step.
     * 
     * @return the last path step.
     */
    public String getLastPathStep()
    {
        return m_path_steps.get(m_path_steps.size() - 1);
    }
    
    /**
     * Returns the length.
     * 
     * @return the length.
     */
    public int getLength()
    {
        return m_path_steps.size();
    }
    
    /**
     * Indicates if the query path is an absolute path.
     * 
     * @return <code>true</code> if the query path is an absolute path, <code>false</code> otherwise.
     */
    public boolean isAbsolute()
    {
        return m_term instanceof AbsoluteVariable;
    }
    
    @Override
    public Type inferType(List<Operator> operators) throws QueryCompilationException
    {                
        Type starting_type = m_term.inferType(operators);
        
        Type found_type = starting_type;
        
        if (starting_type.getClass() == CollectionType.class)
        {
            throw new QueryCompilationException(QueryCompilation.STARTING_WITH_COLLECTION, this.getLocation());
        }
        
        int counter = 1;
        
        // Make sure the query path is valid and does not cross a collection type
        for (String step : m_path_steps)
        {
            SchemaPath schema_path = new SchemaPath(Collections.singletonList(step), PathMode.RELATIVE);
                        
            if (found_type.getClass() == JsonType.class)
            {
                // Any navigation from Json type always succeeds in Json type
                found_type = new JsonType();
                break;
            }
            
            found_type = schema_path.find(found_type);
            
            if (found_type == null)
            {
                throw new QueryCompilationException(QueryCompilation.UNKNOWN_ATTRIBUTE, this.getLocation(), step);
            }
            
            if (counter++ < m_path_steps.size())
            {
                if (found_type.getClass() == CollectionType.class)
                {
                    throw new QueryCompilationException(QueryCompilation.CROSSING_COLLECTION, this.getLocation(), step);
                }
            }
        }
        
        this.setType(found_type);
        
        return this.getType();
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        return m_term.getVariablesUsed();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        return m_term.getFreeParametersUsed();
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return m_term.getBoundParametersUsed();
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        switch (metadata.getDataModel())
        {
            case SQLPLUSPLUS:
                // Always compliant with SQL++ data sources
                break;
            case RELATIONAL:
                // Not always compliant with relational data sources
                boolean compliant = this.getTerm().isDataSourceCompliant(metadata);
                if (!compliant) return false;
                break;
            default:
                throw new AssertionError();
        }
        
        return m_term.isDataSourceCompliant(metadata);
    }
    
    @Override
    public String getDefaultProjectAlias()
    {
        return this.getLastPathStep();
    }
    
    @Override
    public Object[] getDeepEqualityObjects()
    {
        return new Object[] { m_term, m_path_steps };
    }
    
    @Override
    public boolean equals(Object x)
    {
        return EqualUtil.equalsByDeepEquality(this, x);
    }
    
    @Override
    public int hashCode()
    {
        return HashCodeUtil.hashCodeByDeepEquality(this);
    }
    
    @Override
    public String toExplainString()
    {
        return m_term.toExplainString() + PATH_SEPARATOR + StringUtil.join(m_path_steps, PATH_SEPARATOR);
    }
    
    @Override
    public String toString()
    {
        return this.toExplainString();
    }
    
    @Override
    public Term copy()
    {
        QueryPath copy = new QueryPath(m_term.copy(), m_path_steps);
        super.copy(copy);
        
        return copy;
    }
    
    @Override
    public Term copyWithoutType()
    {
        return new QueryPath(this.getTerm().copyWithoutType(), this.getPathSteps());
    }
    
}
