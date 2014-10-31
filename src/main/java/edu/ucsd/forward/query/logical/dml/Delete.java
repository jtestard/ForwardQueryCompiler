/**
 * 
 */
package edu.ucsd.forward.query.logical.dml;

import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * 
 * Represents the logical operator that deletes the tuples in its input tuples.
 * 
 * The Delete operator is assumed to have a Scan operator in its descendants and this Scan operator is called the Target Scan. The
 * Target Scan is needed in order to identify where the primary keys can be found during execution. The RelativeVariable Delete
 * Variable is the alias variable of the Target Scan and is used to infer which operator is the Target Scan in the updateOutputInfo
 * method of this class. The target term is the data object from which tuples will be deleted.
 * 
 * Note that neither the Target Scan nor the Delete Variable can be null.
 * 
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * @author <Vicky Papavasileiou>
 * 
 */
@SuppressWarnings("serial")
public class Delete extends AbstractDmlOperator
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(Delete.class);
    
    private Scan                m_target_scan;
    
    /**
     * The variable of the scan operator.
     */
    private RelativeVariable    m_delete_var;
    
    /**
     * Initializes an instance of the operator.
     * 
     * @param target_data_source
     *            the target data source.
     * @param target_term
     *            the target term to modify.
     * @param delete_var
     *            the variable in the incoming binding that will be used to delete a tuple by calling its getParent() method.
     */
    public Delete(String target_data_source, Term target_term, RelativeVariable delete_var)
    {
        super(target_data_source, target_term);
        
        assert (delete_var != null);
        m_delete_var = delete_var;
    }
    
    /**
     * Initializes an instance of the operator.
     * 
     * @param target_data_source
     *            the target data source.
     * @param target_term
     *            the target term to modify.
     * @param target_scan
     *            the target scan
     */
    public Delete(String target_data_source, Term target_term, RelativeVariable delete_var, Scan target_scan)
    {
        super(target_data_source, target_term);
        m_target_scan = target_scan;
        assert (delete_var != null);
        m_delete_var = delete_var;
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private Delete()
    {
        
    }
    
    /**
     * Gets the variable in the incoming binding that will be used to delete a tuple by calling its getParent() method.
     * 
     * @return a variable.
     */
    public RelativeVariable getDeleteVariable()
    {
        return m_delete_var;
    }
    
    public Scan getTargetScan()
    {
        return m_target_scan;
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        return Collections.<Variable> emptyList();
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        return super.isDataSourceCompliant(metadata);
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        super.updateOutputInfo();
        
        // Get the Scan that scans that output the delete variable
        if (m_target_scan == null) m_target_scan = LogicalPlanUtil.getDefininingScanOperatorWithSubPlans(this, m_delete_var);
        
        assert (m_target_scan != null);
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitDelete(this);
    }
    
    @Override
    public Operator copy()
    {
        Delete copy = new Delete(this.getTargetDataSourceName(), this.getTargetTerm().copy(), m_delete_var);
        super.copy(copy);
        
        return copy;
    }
    
}
