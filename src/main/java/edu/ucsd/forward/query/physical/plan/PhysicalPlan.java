package edu.ucsd.forward.query.physical.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.explain.ExplanationPrinter;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.physical.AbstractAssignImpl;
import edu.ucsd.forward.query.physical.AssignImpl;
import edu.ucsd.forward.query.physical.AssignImplJDBC;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.CopyContext;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.OperatorImpl.State;
import edu.ucsd.forward.query.physical.Pipeline;
import edu.ucsd.forward.query.suspension.SuspensionException;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;
import edu.ucsd.forward.util.Timer;

/**
 * Represents a physical query plan.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class PhysicalPlan implements Pipeline, ExplanationPrinter
{
    private static final Logger log = Logger.getLogger(PhysicalPlan.class);
    
    /**
     * The unique identifier of the physical plan.
     */
    private String              m_id;
    
    /**
     * The root operator implementation of the physical query plan.
     */
    private OperatorImpl        m_root;
    
    /**
     * The state of the operator implementation.
     */
    private State               m_state;
    
    /**
     * The logical plan corresponding to this physical plan.
     */
    private LogicalPlan         m_logical;
    
    /**
     * Determines whether the physical plan has been copied.
     */
    private boolean             m_copied;
    
    /**
     * The assign implementations corresponding to the assign operator's in the logical plan.
     */
    private List<AbstractAssignImpl>    m_assigns;
    
    /**
     * Constructs an instance of the physical query plan.
     * 
     * @param logical
     *            the logical plan corresponding to this physical plan.
     * @param root
     *            the root operator implementation of the physical query plan.
     */
    public PhysicalPlan(OperatorImpl root, LogicalPlan logical)
    {
        assert (root != null);
        m_root = root;
        
        assert (logical != null);
        m_logical = logical;
        
        m_copied = false;
        
        m_id = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.PHYSICAL_PLAN_GENERATOR);
        
        m_state = State.CLOSED;
        
        m_assigns = new ArrayList<AbstractAssignImpl>();
    }
    
    /**
     * Returns the unique identifier of the physical plan.
     * 
     * @return the unique identifier of the physical plan.
     */
    public String getId()
    {
        return m_id;
    }
    
    /**
     * Returns the root operator implementation of the physical plan.
     * 
     * @return the root operator implementation.
     */
    public OperatorImpl getRootOperatorImpl()
    {
        return m_root;
    }
    
    /**
     * Adds an assign implementation.
     * 
     * @param assign_impl
     *            the assign implementation
     */
    public void addAssignImpl(AbstractAssignImpl assign_impl)
    {
        assert assign_impl != null;
        m_assigns.add(assign_impl);
    }
    
    /**
     * Sets the assign implementations.
     * 
     * @param assign_impls
     *            the assign implementations.
     */
    public void setAssignImpl(List<AbstractAssignImpl> assign_impls)
    {
        assert assign_impls != null;
        for (AbstractAssignImpl impl : assign_impls)
            addAssignImpl(impl);
    }
    
    /**
     * Gets all the assign implementations configured.
     * 
     * @return all the assign implementations configured.
     */
    public List<AbstractAssignImpl> getAssignImpls()
    {
        return Collections.unmodifiableList(m_assigns);
    }
    
    /**
     * Clears all the assign implementation configuration.
     */
    public void clearAssignImpls()
    {
        m_assigns.clear();
    }
    
    /**
     * Gets the logical plan corresponding to this physical plan.
     * 
     * @return a logical plan.
     */
    public LogicalPlan getLogicalPlan()
    {
        return m_logical;
    }
    
    /**
     * Determines whether the physical plan has been copied.
     * 
     * @return true, if the physical plan has been copied; otherwise, false.
     */
    public boolean isCopied()
    {
        return m_copied;
    }
    
    /**
     * Returns the state of the physical plan.
     * 
     * @return the state of the physical plan.
     */
    public State getState()
    {
        return m_state;
    }
    
    @Override
    public void open() throws QueryExecutionException
    {
        assert (m_state == State.CLOSED);
        m_state = State.OPEN;
        
        // Open all the assign impls
        for (AbstractAssignImpl assign_impl : getAssignImpls())
        {
            assign_impl.open();
        }
        
        m_root.open();
    }
    
    @Override
    public Binding next() throws QueryExecutionException, SuspensionException
    {
        // Evaluate all Assigns with target source the database
        for (AbstractAssignImpl assign_impl : getAssignImpls())
        {
            if(assign_impl instanceof AssignImplJDBC)
                assign_impl.next();
        }
        
        Binding next = m_root.next();
        
        if (next == null) return null;
        
        return next;
    }
    
    @Override
    public void close() throws QueryExecutionException
    {
        if (m_state == State.CLOSED) return;
        
        // Close recursively all operators
        m_root.close();
        
        // Close all the assign impls.
        for (AbstractAssignImpl assign_impl : getAssignImpls())
        {
            assign_impl.close();
        }
        
        if (!this.getLogicalPlan().isNested())
        {
            // Cleanup the data source accesses
            QueryProcessorFactory.getInstance().cleanup(false);
        }
        
        m_state = State.CLOSED;
    }
    
    @Override
    public String toExplainString()
    {
        return this.toExplainString(m_root, 0);
    }
    
    /**
     * Recursively visits all operator implementations in the physical query plan in a bottom-up fashion.
     * 
     * @param operator_impl
     *            the current operator implementation being visited.
     * @param tabs
     *            the number of indentation tabs.
     * @return the explanation string.
     */
    private String toExplainString(OperatorImpl operator_impl, int tabs)
    {
        String str = "";
        
        
        
        if (!m_assigns.isEmpty())
        {
            for (int i = 0; i < tabs; i++)
                str += "    ";
            str+=" - Assigns:";
            for (AssignImpl assign : m_assigns)
            {
                str+="\n    ";
                for (int i = 0; i < tabs; i++)
                    str += "    ";
                str+=assign.toExplainString();
            }
            str+="\n";
        }
        
//        for (int i = 0; i < tabs; i++)
//            str += "    ";
//        str += operator_impl.getOperator().getOutputInfo().toExplainString() + "\n";
        
        for (int i = 0; i < tabs; i++)
            str += "    ";
        str += operator_impl.toExplainString() + "\n";
        
        for (PhysicalPlan physical_plan : operator_impl.getPhysicalPlansUsed())
        {
            str += this.toExplainString(physical_plan.getRootOperatorImpl(), tabs + 2);
        }
        
        for (OperatorImpl child : operator_impl.getChildren())
        {
            str += this.toExplainString(child, tabs + 1);
        }
        
        return str;
    }
    
    /**
     * Prints statistics of the physical plan in the log.
     */
    public void logStats()
    {
        Set<String> op_impl_names = new HashSet<String>();
        
        log.trace("Plan Size: " + size(m_root, op_impl_names));
        log.trace("Nested Plans: " + Timer.get("Nested Plans"));
        Timer.reset("Nested Plans");
        
        for (String op_impl_name : op_impl_names)
        {
            log.trace(op_impl_name + ": " + Timer.get(op_impl_name));
            Timer.reset(op_impl_name);
        }
    }
    
    /**
     * Gets the number of nodes in the tree rooted at the input operator implementation, including the nodes in nested physical
     * plans.
     * 
     * @param op_impl
     *            the root of the tree.
     * @param op_impl_names
     *            the names of the operators encountered so far.
     * @return the number of nodes in the tree rooted at the input operator implementation, including the nodes in nested physical
     *         plans.
     */
    private int size(OperatorImpl op_impl, Set<String> op_impl_names)
    {
        int result = 1;
        
        op_impl_names.add(op_impl.getName());
        Timer.inc(op_impl.getName(), 1);
        
        for (OperatorImpl child : op_impl.getChildren())
            result += size(child, op_impl_names);
        
        for (PhysicalPlan nested_plan : op_impl.getPhysicalPlansUsed())
        {
            Timer.inc("Nested Plans", 1);
            result += size(nested_plan.getRootOperatorImpl(), op_impl_names);
        }
        
        return result;
    }
    
    /**
     * Creates a copy of the physical plan.
     * 
     * @param context
     *            the copy context
     * @return a copied physical plan.
     */
    public PhysicalPlan copy(CopyContext context)
    {
        assert (!m_copied);
        assert (m_root.getState() == State.CLOSED);
        
        // Copy the assign implementations
        List<AbstractAssignImpl> assigns = new ArrayList<AbstractAssignImpl>();
        for (AbstractAssignImpl impl : getAssignImpls())
        {
            AbstractAssignImpl assign_copy = (AbstractAssignImpl) impl.copy(context);
            assigns.add(assign_copy);
        }
        
        PhysicalPlan copy = new PhysicalPlan(m_root.copy(context), m_logical);
        copy.setAssignImpl(assigns);
        
        // Do not copy parameter instantiations
        copy.m_copied = true;
        
        return copy;
    }
    
}
