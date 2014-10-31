/**
 * 
 */
package edu.ucsd.forward.fpl;

import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.exception.ExceptionMessages.ActionCompilation;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.UncheckedException;
import edu.ucsd.forward.fpl.ast.ActionDefinition;
import edu.ucsd.forward.fpl.ast.Definition;
import edu.ucsd.forward.fpl.ast.FunctionDefinition;
import edu.ucsd.forward.fpl.ast.visitors.FplPlanBuilder;
import edu.ucsd.forward.fpl.plan.FplAction;
import edu.ucsd.forward.fpl.plan.FplFunction;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.ast.function.FunctionNode;
import edu.ucsd.forward.query.ast.function.GeneralFunctionNode;
import edu.ucsd.forward.query.function.FunctionRegistry;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.util.Timer;

/**
 * Represents the action interpreter.
 * 
 * @author Yupeng
 * 
 */
public class FplInterpreter
{
    private static final Logger log = Logger.getLogger(FplInterpreter.class);
    
    /**
     * Hidden constructor.
     * 
     */
    protected FplInterpreter()
    {
        
    }
    
    /**
     * Sets the unified application state.
     * 
     * @param uas
     *            the unified application state.
     */
    public void setUnifiedApplicationState(UnifiedApplicationState uas)
    {
        QueryProcessorFactory.getInstance().setUnifiedApplicationState(uas);
    }
    
    /**
     * Parses FPL code and returns a list of FPL definitions.
     * 
     * @param stmt
     *            the statement to parse.
     * @param location
     *            the location of the statement.
     * @return a list of FPL definitions.
     * @throws FplParsingException
     *             if an error occurs during statement parsing.
     */
    public List<Definition> parseFplCode(String stmt, Location location) throws FplParsingException
    {
        try
        {
            assert (stmt != null);
            
            long time = System.currentTimeMillis();
            
            List<Definition> ast_trees = QueryProcessorFactory.getInstance().parseFplCode(stmt, location);
            
            log.trace("\n<---- Action string ---->\n" + stmt);
            
            Timer.inc("Action Parsing Time", System.currentTimeMillis() - time);
            log.trace("Action Parsing Time: " + Timer.get("Action Parsing Time") + Timer.MS);
            
            return ast_trees;
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Cleanup
            QueryProcessorFactory.getInstance().cleanup(true);
            
            // Throw the expected exception
            if (t instanceof FplParsingException) throw (FplParsingException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    /**
     * Creates an empty FPL function plan for the input FPL function definition and registers it into the FunctionRegistry.
     * 
     * @param func_definition
     *            an FPL function definition.
     * @return an FPL function plan.
     * @throws FplCompilationException
     *             if any compilation error occurs.
     */
    public FplFunction registerFplFunction(FunctionDefinition func_definition) throws FplCompilationException
    {
        assert (func_definition != null);
        
        // First compilation phase, construct function plan and register it in function registry
        FplFunction function_plan = new FplFunction(func_definition);
        try
        {
            FunctionRegistry.getInstance().addApplicationFunction(function_plan);
        }
        catch (FunctionRegistryException e)
        {
            throw new FplCompilationException(ActionCompilation.FUNCTION_REGISTRATION_EXCEPTION, func_definition.getLocation(), e,
                                              func_definition.getName());
        }
        
        return function_plan;
    }
    
    /**
     * Compiles an FPL function definition into an FPL function plan assuming an empty function plan have already been registered
     * into the FunctionRegistry.
     * 
     * @param func_definition
     *            an FPL definition.
     * @param use_top_level_fpl_scope
     *            whether to use the top level fpl scope or not
     * @throws FplCompilationException
     *             if any compilation error occurs.
     */
    public void compileFplFunction(FunctionDefinition func_definition, boolean use_top_level_fpl_scope)
            throws FplCompilationException
    {
        assert (func_definition != null);
        
        // Second compilation phase, build the function plan already registered in function registry
        FplPlanBuilder.build(func_definition, QueryProcessorFactory.getInstance().getUnifiedApplicationState(),
                             use_top_level_fpl_scope);
    }
    
    /**
     * Compiles an FPL function definition into an FPL function plan assuming an empty function plan have already been registered
     * into the FunctionRegistry.
     * 
     * @param func_definition
     *            an FPL definition.
     * @throws FplCompilationException
     *             if any compilation error occurs.
     */
    @Deprecated
    public void compileFplFunction(FunctionDefinition func_definition) throws FplCompilationException
    {
        compileFplFunction(func_definition, false);
    }
    
    /**
     * Compiles an FPL action definition into an FPL action plan.
     * 
     * @param action_definition
     *            an FPL action definition.
     * @return an FPL action.
     * @throws FplCompilationException
     *             if any compilation error occurs.
     */
    public FplAction compileFplAction(ActionDefinition action_definition) throws FplCompilationException
    {
        assert (action_definition != null);
        
        FunctionNode function_node = new GeneralFunctionNode(action_definition.getFunctionName(), action_definition.getLocation());
        AstTree ast_tree = new AstTree(function_node);
        
        UnifiedApplicationState uas = QueryProcessorFactory.getInstance().getUnifiedApplicationState();
        
        // Compile the action plan
        QueryProcessor qp = QueryProcessorFactory.getInstance();
        PhysicalPlan plan = qp.compile(Collections.singletonList(ast_tree), uas).get(0);
        
        return new FplAction(action_definition, plan);
    }
    
    /**
     * Performs the action.
     * 
     * @param action
     *            action
     * @param uas
     *            unified application state
     * @throws FplInterpretationException
     *             exception
     */
    public void performFplAction(FplAction action, UnifiedApplicationState uas) throws FplInterpretationException
    {
        // No need to return any value after performing an action
        QueryProcessorFactory.getInstance().createEagerQueryResult(action.getPlan(), uas).getValue();
    }
    
}
