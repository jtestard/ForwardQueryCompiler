/**
 * 
 */
package edu.ucsd.forward.query.logical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents the operator scanning a term using an index.
 * 
 * @author Yupeng
 * 
 */
@SuppressWarnings("serial")
public class IndexScan extends Scan
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(IndexScan.class);
    
    /**
     * The name of the index.
     */
    private String              m_index_name;
    
    private List<KeyRangeSpec>  m_key_range_specs;
    
    /**
     * Constructs an instance of the index scan operator.
     * 
     * @param alias
     *            the alias of the scan.
     * @param term
     *            the term to scan.
     * @param index_name
     *            the name of the index.
     */
    public IndexScan(String alias, Term term, String index_name)
    {
        super(alias, term);
        assert index_name != null;
        m_index_name = index_name;
        
        m_key_range_specs = new ArrayList<KeyRangeSpec>();
    }
    
    /**
     * Constructs an instance of the index scan operator.
     * 
     * @param alias
     *            the alias of the scan.
     * @param term
     *            the term to scan.
     * @param index_name
     *            the name of the index.
     * @param child
     *            the child operator.
     */
    public IndexScan(String alias, Term term, String index_name, Operator child)
    {
        this(alias, term, index_name);
        
        assert (child != null);
        this.addChild(child);
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private IndexScan()
    {
        
    }
    
    /**
     * Gets the key range specifications.
     * 
     * @return the key range specifications.
     */
    public List<KeyRangeSpec> getKeyRangeSpecs()
    {
        return m_key_range_specs;
    }
    
    /**
     * Adds a key range specification.
     * 
     * @param spec
     *            the key range specification.
     */
    public void addKeyRangeSpec(KeyRangeSpec spec)
    {
        assert spec != null;
        m_key_range_specs.add(spec);
    }
    
    /**
     * Gets the index name.
     * 
     * @return the name of the index.
     */
    public String getIndexName()
    {
        return m_index_name;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitIndexScan(this);
    }
    
    @Override
    public Operator copy()
    {
        IndexScan copy = new IndexScan(this.getAliasVariable().getName(), getTerm().copy(), m_index_name);
        super.copy(copy);
        for (KeyRangeSpec spec : getKeyRangeSpecs())
        {
            KeyRangeSpec spec_copy = new KeyRangeSpec(spec.getLowerTerm(), spec.getUpperTerm(), spec.isLowerOpen(),
                                                      spec.isUpperOpen());
            copy.addKeyRangeSpec(spec_copy);
        }
        
        return copy;
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        super.updateOutputInfo();
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        boolean compliant = true;
        for (KeyRangeSpec spec : getKeyRangeSpecs())
        {
            if (spec.getUpperTerm() != null) compliant &= spec.getUpperTerm().isDataSourceCompliant(metadata);
            if (spec.getLowerTerm() != null) compliant &= spec.getLowerTerm().isDataSourceCompliant(metadata);
        }
        
        return super.isDataSourceCompliant(metadata) && compliant;
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        List<Variable> variables = new ArrayList<Variable>();
        variables.addAll(super.getVariablesUsed());
        for (KeyRangeSpec spec : getKeyRangeSpecs())
        {
            if (spec.getUpperTerm() != null) variables.addAll(spec.getUpperTerm().getVariablesUsed());
            if (spec.getLowerTerm() != null) variables.addAll(spec.getLowerTerm().getVariablesUsed());
        }
        return variables;
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        List<Parameter> parameters = new ArrayList<Parameter>();
        parameters.addAll(super.getFreeParametersUsed());
        for (KeyRangeSpec spec : getKeyRangeSpecs())
        {
            if (spec.getUpperTerm() != null) parameters.addAll(spec.getUpperTerm().getFreeParametersUsed());
            if (spec.getLowerTerm() != null) parameters.addAll(spec.getLowerTerm().getFreeParametersUsed());
        }
        return parameters;
    }
    
    @Override
    public String toExplainString()
    {
        String out = super.toExplainString() + " - " + getTerm().toExplainString() + " - ";
        for (KeyRangeSpec spec : getKeyRangeSpecs())
            out += spec.toString();
        if (this.getAliasVariable().getName() != null)
        {
            out += " AS " + this.getAliasVariable().getName();
        }
        
        return out;
    }
    
    /**
     * Represents a specification of key range on index.
     * 
     * @author Yupeng
     * 
     */
    public static final class KeyRangeSpec implements Serializable
    {
        /**
         * The term for evaluating the upper bound.
         */
        private Term    m_upper_term;
        
        /**
         * The term for evaluating the lower bound.
         */
        private Term    m_lower_term;
        
        private boolean m_lower_open;
        
        private boolean m_upper_open;
        
        /**
         * Default constructor.
         */
        public KeyRangeSpec()
        {
            
        }
        
        /**
         * Constructs the key range specification.
         * 
         * @param lower_term
         *            the term for evaluating the lower bound.
         * @param upper_term
         *            the term for evaluating the upper bound.
         * @param lower_open
         *            indicates if the lower bound is open.
         * @param upper_open
         *            indicates if the upper bound is open.
         */
        public KeyRangeSpec(Term lower_term, Term upper_term, boolean lower_open, boolean upper_open)
        {
            setLowerTerm(lower_term);
            setUpperTerm(upper_term);
            setLowerOpen(lower_open);
            setUpperOpen(upper_open);
        }
        
        /**
         * Sets if the lower bound is open.
         * 
         * @param lower_open
         *            indicates if the lower bound is open.
         */
        public void setLowerOpen(boolean lower_open)
        {
            m_lower_open = lower_open;
        }
        
        /**
         * Sets if the upper bound is open.
         * 
         * @param upper_open
         *            indicates if the upper bound is open.
         */
        public void setUpperOpen(boolean upper_open)
        {
            m_upper_open = upper_open;
        }
        
        /**
         * Gets the term for evaluating the lower bound.
         * 
         * @return term for evaluating the lower bound.
         */
        public Term getLowerTerm()
        {
            return m_lower_term;
        }
        
        /**
         * Sets the term for evaluating the lower bound.
         * 
         * @param lower_term
         *            term for evaluating the lower bound.
         */
        public void setLowerTerm(Term lower_term)
        {
            m_lower_term = lower_term;
        }
        
        /**
         * Gets the term for evaluating the upper bound.
         * 
         * @return term for evaluating the upper bound.
         */
        public Term getUpperTerm()
        {
            return m_upper_term;
        }
        
        /**
         * Sets the term for evaluating the upper bound.
         * 
         * @param upper_term
         *            term for evaluating the upper bound.
         */
        public void setUpperTerm(Term upper_term)
        {
            m_upper_term = upper_term;
        }
        
        /**
         * Checks if the lower bound value is included.
         * 
         * @return <code>true</code> if the lower bound value is included, <code>false</code> otherwise.
         */
        public boolean isLowerOpen()
        {
            return m_lower_open;
        }
        
        /**
         * Checks if the upper bound value is included.
         * 
         * @return <code>true</code> if the upper bound value is included, <code>false</code> otherwise.
         */
        public boolean isUpperOpen()
        {
            return m_upper_open;
        }
        
        @Override
        public String toString()
        {
            String out = "";
            if (getLowerTerm() != null) out += "lower: " + getLowerTerm().toExplainString() + " open:" + isLowerOpen() + " | ";
            if (getUpperTerm() != null) out += "upper: " + getUpperTerm().toExplainString() + " open:" + isUpperOpen();
            return out;
        }
    }
}
