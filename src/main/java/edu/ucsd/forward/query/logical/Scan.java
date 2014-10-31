package edu.ucsd.forward.query.logical;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.LongType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.function.FunctionCall;
import edu.ucsd.forward.query.logical.term.ElementVariable;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.PositionVariable;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;
import edu.ucsd.forward.util.tree.TreePath.PathMode;

/**
 * Represents the operator scanning (iterating over) the elements of a collection.
 * 
 * @author Michalis Petropoulos
 * @author Romain Vernoux
 */
@SuppressWarnings("serial")
public class Scan extends AbstractUnaryOperator
{
    public static final String DEFAULT_ALIAS = "scan_tuple";
    
    /**
     * The term to scan.
     */
    private Term               m_term;
    
    /**
     * The alias of the scan.
     */
    private ElementVariable    m_alias_var;
    
    /**
     * The position variable corresponding to the input order.
     */
    private PositionVariable   m_order_var;
    
    /**
     * Specifies the semantics of the flattening (inner or outer) to use when the Scan is used to unnest collections.
     */
    public enum FlattenSemantics
    {
        INNER, OUTER
    };
    
    private FlattenSemantics m_flatten_semantics = FlattenSemantics.INNER;
    
    /**
     * Constructs an instance of a scan operator.
     * 
     * @param alias
     *            the alias of the scan
     * @param term
     *            the term to scan
     */
    public Scan(String alias, Term term)
    {
        assert (term != null);
        assert (term instanceof Variable || term instanceof QueryPath || term instanceof Parameter || term instanceof FunctionCall);
        m_term = term;
        
        m_alias_var = new ElementVariable((alias == null) ? DEFAULT_ALIAS : alias);
        m_alias_var.setDefaultProjectAlias(term.getDefaultProjectAlias());
        m_alias_var.setLocation(term.getLocation());
    }
    
    /**
     * Constructs an instance of a scan operator.
     * 
     * @param alias
     *            the alias of the scan
     */
    public Scan(String alias)
    {
        m_alias_var = new ElementVariable((alias == null) ? DEFAULT_ALIAS : alias);
    }
    
    /**
     * Default constructor.
     */
    protected Scan()
    {
        
    }
    
    /**
     * Constructs an instance of a scan operator.
     * 
     * @param alias
     *            the alias of the scan.
     * @param term
     *            the term to scan.
     * @param child
     *            the child operator.
     */
    public Scan(String alias, Term term, Operator child)
    {
        this(alias, term);
        
        assert (child != null);
        this.addChild(child);
    }
    
    /**
     * Returns the term to scan.
     * 
     * @return the term to scan.
     */
    public Term getTerm()
    {
        return m_term;
    }
    
    /**
     * Returns the order variable.
     * 
     * @return the order variable if it is set, <code>null</code> otherwise.
     */
    public PositionVariable getOrderVariable()
    {
        return m_order_var;
    }
    
    /**
     * Sets the order variable.
     * 
     * @param pos_var
     *            the relative variable to be set.
     */
    public void setOrderVariable(PositionVariable pos_var)
    {
        assert (pos_var != null);
        m_order_var = pos_var;
    }
    
    /**
     * Sets the term to iterate over.
     * 
     * @param term
     *            the term to set
     */
    public void setTerm(Term term)
    {
        assert (term instanceof Variable || term instanceof QueryPath || term instanceof Parameter || term instanceof FunctionCall);
        if (m_term == null)
        {
            m_alias_var.setDefaultProjectAlias(term.getDefaultProjectAlias());
            m_alias_var.setLocation(term.getLocation());
        }
        m_term = term;
    }
    
    /**
     * Returns the relative variable corresponding to the alias of the scan.
     * 
     * @return a relative variable corresponding to the alias of the scan.
     */
    public ElementVariable getAliasVariable()
    {
        return m_alias_var;
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
        return Collections.emptyList();
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        // Output info
        OutputInfo output_info = new OutputInfo();
        
        // Get the scan term type
        Type scan_type = m_term.inferType(this.getChildren());
        assert (scan_type instanceof CollectionType);
        if (scan_type instanceof CollectionType)
        {
            scan_type = ((CollectionType) scan_type).getChildrenType();
        }
        
        // Compute the output info
        // First using the input variables
        int index = 0;
        for (RelativeVariable in_var : this.getInputVariables())
        {
            output_info.add(in_var, in_var);
            in_var.setBindingIndex(index++);
        }
        
        // Then add the element variable
        m_alias_var.setType(scan_type);
        m_alias_var.setBindingIndex(index++);
        output_info.add(m_alias_var, m_term);
        
        // The add the position variable
        if (m_order_var != null)
        {
            m_order_var.setType(new LongType());
            m_order_var.setBindingIndex(index++);
            output_info.add(m_order_var, m_term);
        }
        
        this.setOutputInfo(output_info);
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        // If the scan outputs the order of the input collection, it has to be in memory.
        if (m_order_var != null)
        {
            if (metadata.getStorageSystem() == StorageSystem.INMEMORY)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        
        return m_term.isDataSourceCompliant(metadata);
    }
    
    @Override
    public String toExplainString()
    {
        String out = super.toExplainString() + " on " + m_term.toExplainString();
        
        out += " " + m_alias_var.getName();
        
        if (m_order_var != null)
        {
            out += " position as " + m_order_var;
        }
        
        return out;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitScan(this);
    }
    
    @Override
    public Operator copy()
    {
        Scan copy = new Scan(m_alias_var.getName(), m_term.copy());
        
        if (m_order_var != null)
        {
            copy.setOrderVariable((PositionVariable) m_order_var.copy());
        }
        
        super.copy(copy);
        
        return copy;
    }
    
    @Override
    public Scan copyWithoutType()
    {
        Scan scan = new Scan(m_alias_var.getName(), m_term.copyWithoutType());
        
        if (m_order_var != null)
        {
            scan.setOrderVariable((PositionVariable) m_order_var.copy());
        }
        
        return scan;
    }
    
    /**
     * Returns the scan term alias. If it is not set, use the default one.
     * 
     * @return the scan term alias
     */
    public String getAlias()
    {
        return m_alias_var.getName();
    }
    
    /**
     * Set the flattening semantics to use when the Scan is used to unnest collections.
     * 
     * @param flatten_semantics
     *            the semantics to use
     */
    public void setFlattenSemantics(FlattenSemantics flatten_semantics)
    {
        m_flatten_semantics = flatten_semantics;
    }
    
    /**
     * Return the flattening semantics to use when the Scan is used to unnest collections.
     * 
     * @return the flattening semantics to use
     */
    public FlattenSemantics getFlattenSemantics()
    {
        return m_flatten_semantics;
    }
    
}
