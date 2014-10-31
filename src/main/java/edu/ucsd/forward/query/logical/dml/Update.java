/**
 * 
 */
package edu.ucsd.forward.query.logical.dml;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeConverter;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.NestedPlansOperator;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents the logical operator that updates attributes of tuples in its input to new values. Update may have a child operator
 * that provides it with the tuples that must be updated. If there is no child operator other than Ground, this means that either a
 * single Scalar data object is being updated or that the entire data object is being replaced.
 * 
 * The Update operator consists of Assignments that define the target of the update i.e., the attribute that will get updated and a
 * subPlan that when evaluated returns the new value for the particular attribute. The operator will have one Assignment for each of
 * the attributes that will get updated. Note that if the entire data object is being updated, then the target of the Assignment is
 * the data object itself.
 * 
 * Besides the Assignments, the Update has a list of parameters that it needs to instantiate. These parameters are the free
 * parameters of the subPlans in each Assignment. Moreover, if the Update has a Scan operator in its descendants, this Scan operator
 * is called the Target Scan. The Target Scan is needed in order to identify where the primary keys can be found during execution.
 * The RelativeVariable Update Variable is the alias variable of the Target Scan and is used to infer which the operator is the
 * Target Scan in the updateOutputInfo method. The Target Term is the data object from which tuples will be deleted.
 * 
 * Note that both the Target Scan as well as the Update Variable can be null.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * @author <Vicky Papavasileiou>
 */
@SuppressWarnings("serial")
public class Update extends AbstractDmlOperator implements NestedPlansOperator
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(Update.class);
    
    /**
     * The list of assignments.
     */
    private List<Assignment>    m_assignments;
    
    /**
     * The bound parameters.
     */
    private List<Parameter>     m_bound_params;
    
    /**
     * The Scan operator that iterates over the target data object.
     */
    private Scan                m_target_scan;
    
    /**
     * The variable of the scan operator.
     */
    private RelativeVariable    m_update_var;
    
    /**
     * Constructs the operator.
     * 
     * @param target_data_source
     *            the target data source.
     * @param target_term
     *            the target term to modify.
     */
    public Update(String target_data_source, Term target_term)
    {
        super(target_data_source, target_term);
        
        m_assignments = new ArrayList<Assignment>();
    }
    
    /**
     * Initializes an instance of the operator.
     * 
     * @param target_data_source
     *            the target data source.
     * @param target_term
     *            the target term to modify.
     */
    public Update(String target_data_source, Term target_term, RelativeVariable update_var, Scan target_scan)
    {
        super(target_data_source, target_term);
        m_assignments = new ArrayList<Assignment>();
        m_target_scan = target_scan;
        m_update_var = update_var;
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private Update()
    {
        
    }
    
    /**
     * Gets the assignments.
     * 
     * @return the assignments
     */
    public List<Assignment> getAssignments()
    {
        return m_assignments;
    }
    
    /**
     * Adds one assignment.
     * 
     * @param assignment
     *            the assignment
     */
    public void addAssignment(Assignment assignment)
    {
        assert assignment != null;
        m_assignments.add(assignment);
    }
    
    /**
     * Return the Scan operator that scans the data object that is the subject of the update.
     * 
     * @return the target scan operator.
     */
    public Scan getTargetScan()
    {
        return m_target_scan;
    }
    
    public RelativeVariable getUpdateVariable()
    {
        return m_update_var;
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        List<Variable> vars = new ArrayList<Variable>();
        
        for (Assignment assignment : m_assignments)
        {
            vars.addAll(assignment.getTerm().getVariablesUsed());
        }
        
        return vars;
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
        for (Assignment assignment : m_assignments)
            m_bound_params.addAll(assignment.getLogicalPlan().getFreeParametersUsed());
    }
    
    @Override
    public List<LogicalPlan> getLogicalPlansUsed()
    {
        List<LogicalPlan> plans = new ArrayList<LogicalPlan>();
        for (Assignment assignment : m_assignments)
        {
            plans.add(assignment.getLogicalPlan());
        }
        
        return plans;
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        super.updateOutputInfo();
        
        // Get the Scan that scans that output the update variable
        if (m_target_scan == null && m_update_var != null)
        {
            m_target_scan = LogicalPlanUtil.getDefininingScanOperatorWithSubPlans(this, m_update_var);
        }
        
        // The target scan can be null if the data object that is updated is a single scalar or the entire data object is replaced
        
        for (Assignment assignment : m_assignments)
        {
            // Check type compatibility
            Type expected = assignment.getTerm().inferType(this.getChildren());
            
            QueryProcessor processor = QueryProcessorFactory.getInstance();
            Type assigned = processor.getOutputType(assignment.getLogicalPlan(), processor.getUnifiedApplicationState());
            
            // FIXME Include constraints in the check
            boolean safe = TypeUtil.deepEqualsByIsomorphism(assigned, expected, false);
            if (!safe)
            {
                safe = TypeConverter.getInstance().canImplicitlyConvert(assigned, expected);
            }
            
            // Incompatible types
            if (!safe)
            {
                throw new QueryCompilationException(QueryCompilation.DML_INVALID_PROVIDED_TYPE,
                                                    assignment.getTerm().getLocation(), assignment.getTerm().toString());
            }
        }
        
        // Set the bound parameters
        this.setBoundParametersUsed();
        
        // Validate the bound parameters
        for (Parameter param : this.getBoundParametersUsed())
            param.getTerm().inferType(this.getChildren());
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        boolean compliant = super.isDataSourceCompliant(metadata);
        switch (metadata.getDataModel())
        {
            case SQLPLUSPLUS:
                return compliant;
            case RELATIONAL:
                for (Assignment assignment : m_assignments)
                {
                    if (!assignment.getTerm().isDataSourceCompliant(metadata)) return false;
                    if (!assignment.getLogicalPlan().isDataSourceCompliant(metadata)) return false;
                }
                
                return compliant;
            default:
                throw new AssertionError();
        }
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitUpdate(this);
    }
    
    /**
     * Represents an assignment of the update operator.
     * 
     * @author Yupeng
     * 
     */
    public static final class Assignment implements Serializable
    {
        /**
         * The term of the assignment that navigates to the target.
         */
        private Term        m_term;
        
        /**
         * The logical plan of the option that specifies the update source.
         */
        private LogicalPlan m_plan;
        
        /**
         * Constructs an instance of the assignment.
         * 
         * @param term
         *            the target of the assignment
         * @param plan
         *            the update source of the assignment
         */
        public Assignment(Term term, LogicalPlan plan)
        {
            assert term != null;
            assert plan != null;
            m_term = term;
            m_plan = plan;
        }
        
        @SuppressWarnings("unused")
        private Assignment()
        {
            
        }
        
        /**
         * Returns the term to the target.
         * 
         * @return the term to the target.
         */
        public Term getTerm()
        {
            return m_term;
        }
        
        /**
         * Sets the term to the target.
         * 
         * @param term
         *            the term to the target.
         */
        public void setTerm(Term term)
        {
            assert (term != null);
            m_term = term;
        }
        
        /**
         * Returns the logical plan of the option.
         * 
         * @return the logical plan of the option.
         */
        public LogicalPlan getLogicalPlan()
        {
            return m_plan;
        }
    }
    
    @Override
    public Operator copy()
    {
        Update copy = new Update(this.getTargetDataSourceName(), this.getTargetTerm().copy());
        super.copy(copy);
        
        for (Assignment assignment : getAssignments())
        {
            copy.addAssignment(new Assignment(assignment.getTerm().copy(), assignment.getLogicalPlan().copy()));
        }
        return copy;
    }
    
}
