/**
 * 
 */
package edu.ucsd.forward.fpl.ast.visitors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.lang.StringUtils;

import edu.ucsd.app2you.util.collection.Pair;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.exception.ExceptionMessages.ActionCompilation;
import edu.ucsd.forward.fpl.FplCompilationException;
import edu.ucsd.forward.fpl.ast.ActionDefinition;
import edu.ucsd.forward.fpl.ast.ActionInvocation;
import edu.ucsd.forward.fpl.ast.AssignmentStatement;
import edu.ucsd.forward.fpl.ast.Body;
import edu.ucsd.forward.fpl.ast.Branch;
import edu.ucsd.forward.fpl.ast.DeclarationSection;
import edu.ucsd.forward.fpl.ast.DynamicActionInvocation;
import edu.ucsd.forward.fpl.ast.ExceptionDeclaration;
import edu.ucsd.forward.fpl.ast.ExceptionHandler;
import edu.ucsd.forward.fpl.ast.FunctionDefinition;
import edu.ucsd.forward.fpl.ast.IfStatement;
import edu.ucsd.forward.fpl.ast.ParameterDeclaration;
import edu.ucsd.forward.fpl.ast.RaiseStatement;
import edu.ucsd.forward.fpl.ast.ReturnStatement;
import edu.ucsd.forward.fpl.ast.Statement;
import edu.ucsd.forward.fpl.ast.StaticActionInvocation;
import edu.ucsd.forward.fpl.ast.VariableDeclaration;
import edu.ucsd.forward.fpl.plan.ActionCall;
import edu.ucsd.forward.fpl.plan.ConditionalJumpInstruction;
import edu.ucsd.forward.fpl.plan.DynamicActionCall;
import edu.ucsd.forward.fpl.plan.FplFunction;
import edu.ucsd.forward.fpl.plan.Instruction;
import edu.ucsd.forward.fpl.plan.NextInstruction;
import edu.ucsd.forward.fpl.plan.QueryInstruction;
import edu.ucsd.forward.fpl.plan.ReturnInstruction;
import edu.ucsd.forward.fpl.plan.StaticActionCall;
import edu.ucsd.forward.fpl.plan.UnconditionalJumpInstruction;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.ast.AttributeReference;
import edu.ucsd.forward.query.ast.QueryStatement;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.dml.UpdateStatement;
import edu.ucsd.forward.query.ast.function.GeneralFunctionNode;
import edu.ucsd.forward.query.ast.literal.BooleanLiteral;
import edu.ucsd.forward.query.function.Function;
import edu.ucsd.forward.query.function.FunctionRegistry;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.function.comparison.EqualFunction;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;
import edu.ucsd.forward.warning.FplCompilationWarning;
import edu.ucsd.forward.warning.WarningCollectorFactory;

/**
 * The action AST node visitor that converts a function definition to a function plan.
 * 
 * @author Yupeng
 * 
 */
public final class FplPlanBuilder extends AbstractFplAstNodeVisitor
{
    @SuppressWarnings("unused")
    private static final Logger                         log = Logger.getLogger(FplPlanBuilder.class);
    
    /**
     * The unified application state.
     */
    private UnifiedApplicationState                     m_uas;
    
    private FunctionDefinition                          m_definition;
    
    private FplFunction                                 m_plan;
    
    /**
     * Indicates the mapping between the declared exception and its corresponding handler.
     */
    private Map<ExceptionDeclaration, ExceptionHandler> m_declaration_handlers;
    
    /**
     * Indicates the labels for the control flow stack.
     */
    private Stack<String>                               m_control_flow_labels;
    
    /**
     * Indicates if current control flow of function has returned.
     */
    private boolean                                     m_returned;
    
    /**
     * Indicates a list of labels that will be added to the instruction added next.
     */
    private List<String>                                m_pending_labels;
    
    private boolean                                     m_use_top_level_fpl_scope;
    
    /**
     * Constructs a function plan builder.
     * 
     * @param definition
     *            the function definition to translate.
     * @param uas
     *            the unified application state.
     * @param use_top_level_fpl_scope
     *            whether to use the top level fpl scope or not
     */
    private FplPlanBuilder(FunctionDefinition definition, UnifiedApplicationState uas, boolean use_top_level_fpl_scope)
    {
        assert (uas != null);
        
        m_definition = definition;
        m_uas = uas;
        m_declaration_handlers = new HashMap<ExceptionDeclaration, ExceptionHandler>();
        m_control_flow_labels = new Stack<String>();
        m_returned = false;
        m_pending_labels = new ArrayList<String>();
        m_use_top_level_fpl_scope = use_top_level_fpl_scope;
    }
    
    /**
     * Builds a function definition into an empty function plan that has been registered to the FunctionRegistry.
     * 
     * @param definition
     *            the function definition to translate.
     * @param uas
     *            the unified application state.
     * @return a function plan built
     * @throws FplCompilationException
     *             if any error occurs during compilation.
     */
    public static FplFunction build(FunctionDefinition definition, UnifiedApplicationState uas) throws FplCompilationException
    {
        return build(definition, uas, false);
    }
    
    /**
     * Builds a function definition into an empty function plan that has been registered to the FunctionRegistry.
     * 
     * @param definition
     *            the function definition to translate.
     * @param uas
     *            the unified application state.
     * @param use_top_level_fpl_scope
     *            whether to use the top level fpl scope or not
     * @return a function plan built
     * @throws FplCompilationException
     *             if any error occurs during compilation.
     */
    public static FplFunction build(FunctionDefinition definition, UnifiedApplicationState uas, boolean use_top_level_fpl_scope)
            throws FplCompilationException
    {
        NameGeneratorFactory.getInstance().reset(NameGenerator.FUNCTION_PLAN_GENERATOR);
        
        FplPlanBuilder converter = new FplPlanBuilder(definition, uas, use_top_level_fpl_scope);
        
        try
        {
            Function function = FunctionRegistry.getInstance().getFunction(definition.getName());
            assert (function instanceof FplFunction);
            converter.m_plan = (FplFunction) function;
            converter.m_plan.clear();
        }
        catch (FunctionRegistryException e)
        {
            // This should never happen
            throw new AssertionError();
        }
        
        Object obj = null;
        try
        {
            converter.before(uas);
            obj = definition.accept(converter);
        }
        finally
        {
            converter.after(uas);
        }
        
        return (FplFunction) obj;
    }
    
    /**
     * Prepares the local data source for the function compilation by creating schema objects for the declared variable and the
     * parameters.
     * 
     * @param uas
     *            the unified application state.
     * @throws FplCompilationException
     *             if anything wrong occurs during the preparation.
     */
    private void before(UnifiedApplicationState uas) throws FplCompilationException
    {
        // Request a new frame in storage stack only if we are not using top level
        if (!m_use_top_level_fpl_scope)
        {
            try
            {
                uas.requestNewFplSource();
            }
            catch (QueryExecutionException e1)
            {
                // This should never happen
                throw new AssertionError();
            }
        }
        
        // Create schema tree and data tree for the parameters and the declared variables
        TupleType data_obj_type = new TupleType();
        
        // Create a tuple value containing the parameters and the declared variables
        Set<String> seen_names = new HashSet<String>();
        for (ParameterDeclaration declaration : m_definition.getParameterDeclarations())
        {
            String name = declaration.getName();
            // Check if the parameter name exists
            if (seen_names.contains(name))
            {
                throw new FplCompilationException(ActionCompilation.DUPLICATE_PARAMETER_NAME, declaration.getLocation(), name);
            }
            seen_names.add(name);
            data_obj_type.setAttribute(name, TypeUtil.cloneNoParent(declaration.getType()));
            // schema_obj.setAllowDirectReference(true);
        }
        if (m_definition.getDeclarationSection() != null)
        {
            seen_names.clear();
            for (VariableDeclaration declaration : m_definition.getDeclarationSection().getVariableDeclarations())
            {
                String name = declaration.getName();
                // Check if the parameter name exists
                if (seen_names.contains(name))
                {
                    throw new FplCompilationException(ActionCompilation.DUPLICATE_VARIABLE_NAME, declaration.getLocation(), name);
                }
                seen_names.add(name);
                data_obj_type.setAttribute(name, TypeUtil.cloneNoParent(declaration.getType()));
                // schema_obj.setAllowDirectReference(true);
            }
        }
        
        try
        {
            // Create a schema object in the FPL local scope data source
            String schema_obj_name = m_definition.getName();
            uas.getDataSource(DataSource.FPL_LOCAL_SCOPE).createSchemaObject(schema_obj_name, new SchemaTree(data_obj_type),
                                                                             Size.SMALL);
        }
        catch (QueryExecutionException e)
        {
            // This should never happen
            throw new AssertionError();
        }
        catch (DataSourceException e)
        {
            // This should never happen
            throw new AssertionError();
        }
    }
    
    /**
     * Cleans after the function compilation by removing the local data source. If we are using top level fpl scope, then we assume
     * the caller will take care of popping the top level fpl source.
     * 
     * @param uas
     *            the unified application state.
     */
    private void after(UnifiedApplicationState uas)
    {
        if (m_use_top_level_fpl_scope) return;
        
        // Drop the schema object in the FPL local scope data source
        String schema_obj_name = m_definition.getName();
        try
        {
            if (uas.getDataSource(DataSource.FPL_LOCAL_SCOPE).hasSchemaObject(schema_obj_name))
            {
                uas.getDataSource(DataSource.FPL_LOCAL_SCOPE).dropSchemaObject(schema_obj_name);
            }
            
            if (m_use_top_level_fpl_scope) return;
            
            // Remove the top frame
            uas.removeTopFplSource();
        }
        catch (DataSourceException e)
        {
            // This should never happen
            throw new AssertionError();
        }
        catch (QueryExecutionException e)
        {
            // This should never happen
            throw new AssertionError();
        }
    }
    
    /**
     * Adds one instruction.
     * 
     * @param instruction
     *            the instruction to add.
     */
    private void addInstructionToPlan(Instruction instruction)
    {
        assert instruction != null;
        for (String label : m_pending_labels)
        {
            instruction.addLabel(label);
        }
        m_pending_labels.clear();
        m_plan.addInstruction(instruction);
    }
    
    @Override
    public Object visitActionDefinition(ActionDefinition ast_node) throws FplCompilationException
    {
        throw new AssertionError();
    }
    
    @Override
    public Object visitBranch(Branch ast_node) throws FplCompilationException
    {
        String next_label = m_control_flow_labels.pop();
        String end_label = m_control_flow_labels.peek();
        // FIXME: We don't have short-circuit support.
        ValueExpression condition = ast_node.getCondition();
        AstTree tree = new AstTree((ValueExpression) condition.copy());
        Type return_plan_type = QueryProcessorFactory.getInstance().getOutputType(tree, m_uas);
        if (!(return_plan_type instanceof BooleanType))
        {
            throw new FplCompilationException(ActionCompilation.INVALID_IF_CONDITION_TYPE, condition.getLocation(),
                                              condition.toString());
        }
        ConditionalJumpInstruction instruction = new ConditionalJumpInstruction(false, buildQueryPlan(condition));
        this.addInstructionToPlan(instruction);
        instruction.setTargetLabel(next_label);
        for (Statement statement : ast_node.getStatements())
        {
            convertStatement(statement);
        }
        // Explicit jump to the end of the IF statement.
        UnconditionalJumpInstruction goto_end = new UnconditionalJumpInstruction(end_label);
        this.addInstructionToPlan(goto_end);
        return null;
    }
    
    @Override
    public Object visitExceptionDeclaration(ExceptionDeclaration ast_node) throws FplCompilationException
    {
        String exception_name = ast_node.getExceptionName();
        for (ExceptionDeclaration declaration : m_declaration_handlers.keySet())
        {
            if (declaration.getExceptionName().equals(ast_node.getExceptionName()))
            {
                throw new FplCompilationException(ActionCompilation.DUPLICATE_EXCEPTION, ast_node.getLocation(), exception_name);
            }
        }
        m_declaration_handlers.put(ast_node, null);
        return null;
    }
    
    @Override
    public Object visitExceptionHandler(ExceptionHandler ast_node) throws FplCompilationException
    {
        if (ast_node.getExceptionNames().isEmpty())
        {
            // Handle others
            for (ExceptionDeclaration declaration : m_declaration_handlers.keySet())
            {
                if (m_declaration_handlers.get(declaration) == null)
                {
                    m_declaration_handlers.put(declaration, ast_node);
                    // Adds a pending label to be marked on the next instruction
                    m_pending_labels.add(declaration.getExceptionName());
                }
            }
        }
        else
        {
            for (String exception_name : ast_node.getExceptionNames())
            {
                boolean found = false;
                for (ExceptionDeclaration declaration : m_declaration_handlers.keySet())
                {
                    if (declaration.getExceptionName().equals(exception_name))
                    {
                        found = true;
                        m_declaration_handlers.put(declaration, ast_node);
                    }
                }
                if (!found)
                {
                    throw new FplCompilationException(ActionCompilation.HANDLED_EXCEPTION_NOT_DECLARED, ast_node.getLocation(),
                                                      exception_name);
                }
                
                m_pending_labels.add(exception_name);
            }
        }
        for (Statement statement : ast_node.getStatements())
        {
            convertStatement(statement);
        }
        // Check if the handler has returned
        if (!m_returned)
        {
            if (m_definition.getReturnType() == null)
            {
                // Add a return instruction.
                ReturnInstruction instruction = new ReturnInstruction();
                this.addInstructionToPlan(instruction);
            }
            else
            {
                throw new FplCompilationException(ActionCompilation.MISSING_RETURN_STATEMENT_IN_HANDLER, ast_node.getLocation(),
                                                  m_definition.getReturnType());
            }
        }
        m_returned = false;
        return null;
    }
    
    @Override
    public Object visitFunctionBody(Body ast_node) throws FplCompilationException
    {
        for (Statement statement : ast_node.getStatements())
        {
            convertStatement(statement);
        }
        // Check if the function has returned
        if (!m_returned)
        {
            if (m_definition.getReturnType() == null)
            {
                // Add a return instruction.
                ReturnInstruction instruction = new ReturnInstruction();
                this.addInstructionToPlan(instruction);
            }
            else
            {
                throw new FplCompilationException(ActionCompilation.MISSING_RETURN_STATEMENT, m_definition.getLocation(),
                                                  m_definition.getReturnType());
            }
        }
        m_returned = false;
        for (ExceptionHandler handler : ast_node.getExceptionHandlers())
        {
            handler.accept(this);
        }
        // Check all the declared exceptions are handled
        for (ExceptionDeclaration declaration : m_declaration_handlers.keySet())
        {
            if (m_declaration_handlers.get(declaration) == null)
            {
                throw new FplCompilationException(ActionCompilation.UNHANDLED_EXCEPTION, declaration.getLocation(),
                                                  declaration.getExceptionName());
            }
        }
        return null;
    }
    
    /**
     * Converts a statement.
     * 
     * @param statement
     *            the statement to converted
     * @throws FplCompilationException
     *             if any error occurs during compilation.
     */
    private void convertStatement(Statement statement) throws FplCompilationException
    {
        // Check if the statement is reachable
        if (m_returned)
        {
            throw new FplCompilationException(ActionCompilation.UNREACHABLE_STATEMENT, statement.getLocation(),
                                              statement.toString());
        }
        
        if (statement instanceof QueryStatement)
        {
            Instruction instruction = new QueryInstruction(buildQueryPlan((QueryStatement) statement));
            // warning check
            if ((statement instanceof GeneralFunctionNode)
                    && ((GeneralFunctionNode) statement).getFunctionName().equals(EqualFunction.NAME))
            {
                WarningCollectorFactory.getInstance().add(new FplCompilationWarning(
                                                                                    edu.ucsd.forward.warning.WarningMessages.ActionCompilation.TYPO_IN_ASSIGNMENT,
                                                                                    statement.getLocation()));
            }
            
            this.addInstructionToPlan(instruction);
        }
        else statement.accept(this);
    }
    
    @Override
    public Object visitFunctionDeclarationSection(DeclarationSection ast_node) throws FplCompilationException
    {
        for (VariableDeclaration declaration : ast_node.getVariableDeclarations())
        {
            declaration.accept(this);
        }
        for (ExceptionDeclaration declaration : ast_node.getExceptionDeclarations())
        {
            declaration.accept(this);
        }
        return null;
    }
    
    @Override
    public Object visitFunctionDefinition(FunctionDefinition ast_node) throws FplCompilationException
    {
        for (ParameterDeclaration declaration : ast_node.getParameterDeclarations())
        {
            declaration.accept(this);
        }
        if (ast_node.getDeclarationSection() != null) ast_node.getDeclarationSection().accept(this);
        ast_node.getFunctionBody().accept(this);
        return m_plan;
    }
    
    @Override
    public Object visitIfStatement(IfStatement ast_node) throws FplCompilationException
    {
        String end_label = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.FUNCTION_PLAN_GENERATOR);
        m_control_flow_labels.push(end_label);
        
        // Check if the ELSE branch exists
        ValueExpression last_branch_cond = ast_node.getBranches().get(ast_node.getBranches().size() - 1).getCondition();
        boolean all_branches_returned = (last_branch_cond instanceof BooleanLiteral && ((BooleanLiteral) last_branch_cond).getScalarValue().getObject());
        
        for (Branch branch : ast_node.getBranches())
        {
            String next_label = NameGeneratorFactory.getInstance().getUniqueName(NameGenerator.FUNCTION_PLAN_GENERATOR);
            m_control_flow_labels.push(next_label);
            // Mark the branch as unreturned
            m_returned = false;
            branch.accept(this);
            if (!m_returned) all_branches_returned = false;
            NextInstruction instruction = new NextInstruction(next_label);
            this.addInstructionToPlan(instruction);
        }
        m_returned = all_branches_returned;
        m_control_flow_labels.pop();
        NextInstruction instruction = new NextInstruction(end_label);
        this.addInstructionToPlan(instruction);
        return null;
    }
    
    @Override
    public Object visitParameterDeclaration(ParameterDeclaration ast_node) throws FplCompilationException
    {
        return null;
    }
    
    @Override
    public Object visitRaiseStatement(RaiseStatement ast_node) throws FplCompilationException
    {
        String exception_name = ast_node.getExceptionName();
        boolean found = false;
        for (ExceptionDeclaration declaration : m_declaration_handlers.keySet())
        {
            if (declaration.getExceptionName().equals(exception_name))
            {
                found = true;
            }
        }
        if (!found)
        {
            throw new FplCompilationException(ActionCompilation.UNDECLARED_EXCEPTION, ast_node.getLocation(), exception_name);
        }
        UnconditionalJumpInstruction instruction = new UnconditionalJumpInstruction(exception_name);
        this.addInstructionToPlan(instruction);
        m_returned = true;
        return null;
    }
    
    @Override
    public Object visitReturnStatement(ReturnStatement ast_node) throws FplCompilationException
    {
        ReturnInstruction instruction = new ReturnInstruction();
        Type return_plan_type = null;
        if (ast_node.getExpression() != null)
        {
            instruction.setQueryPlan(buildQueryPlan(ast_node.getExpression()));
            AstTree tree = new AstTree((ValueExpression) ast_node.getExpression().copy());
            return_plan_type = QueryProcessorFactory.getInstance().getOutputType(tree, m_uas);
        }
        // Check if the return type matches
        Type return_type = m_definition.getReturnType();
        if (return_type == null && return_plan_type != null)
        {
            throw new FplCompilationException(ActionCompilation.INVALID_RETURN_TYPE, ast_node.getLocation(), "void",
                                              return_plan_type);
        }
        else if (return_type != null && ast_node.getExpression() == null)
        {
            throw new FplCompilationException(ActionCompilation.INVALID_RETURN_TYPE, ast_node.getLocation(), return_type, "void");
        }
        else if (!TypeUtil.deepEqualsByIsomorphism(return_type, return_plan_type))
        {
            throw new FplCompilationException(ActionCompilation.INVALID_RETURN_TYPE, ast_node.getLocation(), return_type,
                                              return_plan_type);
        }
        
        this.addInstructionToPlan(instruction);
        m_returned = true;
        return null;
    }
    
    /**
     * Compiles a query plan from a specified query statement.
     * 
     * @param query
     *            the query statement to compile.
     * @return the built query plan.
     * @throws QueryCompilationException
     *             if any error occurs during compilation.
     */
    private PhysicalPlan buildQueryPlan(QueryStatement query) throws QueryCompilationException
    {
        AstTree ast_tree = new AstTree(query);
        List<AstTree> ast_trees = new ArrayList<AstTree>();
        ast_trees.add(ast_tree);
        PhysicalPlan physical_plan = QueryProcessorFactory.getInstance().compile(ast_trees, m_uas).get(0);
        physical_plan.getLogicalPlan().setNested(true);
        return physical_plan;
    }
    
    @Override
    public Object visitVariableDeclaration(VariableDeclaration ast_node) throws FplCompilationException
    {
        if (ast_node.getDefaultExpression() != null)
        {
            // Adds DML statement for setting variable to default value.
            UpdateStatement statement = new UpdateStatement(ast_node.getLocation());
            ArrayList<Pair<AttributeReference, ValueExpression>> setList = new ArrayList<Pair<AttributeReference, ValueExpression>>();
            // AttributeReference target = new AttributeReference(m_definition.getName(), ast_node.getName());
            AttributeReference target = new AttributeReference(Collections.singletonList(ast_node.getName()),
                                                               ast_node.getLocation());
            // Check the type
            Type variable_type = ast_node.getType();
            AstTree tree = new AstTree((ValueExpression) ast_node.getDefaultExpression().copy());
            Type default_type = QueryProcessorFactory.getInstance().getOutputType(tree, m_uas);
            if (!TypeUtil.deepEqualsByIsomorphism(variable_type, default_type))
            {
                throw new FplCompilationException(ActionCompilation.INVALID_VARIABLE_DEFAULT_VALUE_TYPE, ast_node.getLocation(),
                                                  variable_type.toString(), default_type.toString());
            }
            
            ValueExpression expr = (ValueExpression) ast_node.getDefaultExpression().copy();
            setList.add(new Pair<AttributeReference, ValueExpression>(target, expr));
            statement.setUpdateList(setList);
            convertStatement(statement);
        }
        return null;
    }
    
    @Override
    public Object visitAssignmentStatement(AssignmentStatement ast_node) throws FplCompilationException
    {
        // Converts the assignment statement to UPDATE statement
        UpdateStatement statement = new UpdateStatement(ast_node.getLocation());
        ArrayList<Pair<AttributeReference, ValueExpression>> setList = new ArrayList<Pair<AttributeReference, ValueExpression>>();
        AttributeReference target = (AttributeReference) ast_node.getTarget().copy();
        ValueExpression expr = (ValueExpression) ast_node.getExpression().copy();
        // Check the type
        AstTree tree = new AstTree((ValueExpression) ast_node.getExpression().copy());
        Type expr_type = QueryProcessorFactory.getInstance().getOutputType(tree, m_uas);
        tree = new AstTree((ValueExpression) ast_node.getTarget().copy());
        Type target_type = QueryProcessorFactory.getInstance().getOutputType(tree, m_uas);
        if (!TypeUtil.deepEqualsByIsomorphism(target_type, expr_type))
        {
            throw new FplCompilationException(ActionCompilation.UNMATCH_ASSIGNMENT_TYPE, ast_node.getLocation(),
                                              target_type.toString(), expr_type.toString());
        }
        setList.add(new Pair<AttributeReference, ValueExpression>(target, expr));
        statement.setUpdateList(setList);
        convertStatement(statement);
        return null;
    }
    
    @Override
    public Object visitActionInvocation(ActionInvocation ast_node) throws FplCompilationException
    {
        // Compile the action invocation arguments
        List<PhysicalPlan> args = new ArrayList<PhysicalPlan>();
        for (ValueExpression arg : ast_node.getInvocationArguments())
            args.add(buildQueryPlan(arg));
        
        // Construct the action call
        ActionCall instruction = null;
        if (ast_node instanceof StaticActionInvocation)
        {
            String action_name = ((StaticActionInvocation) ast_node).getActionPath();
            instruction = new StaticActionCall(ast_node.getHttpVerb(), ast_node.inNewWindow(), action_name, args);
        }
        else if (ast_node instanceof DynamicActionInvocation)
        {
            ValueExpression action_name = ((DynamicActionInvocation) ast_node).getActionPath();
            instruction = new DynamicActionCall(ast_node.getHttpVerb(), ast_node.inNewWindow(), buildQueryPlan(action_name), args);
        }
        else
        {
            throw new AssertionError();
        }
        
        this.addInstructionToPlan(instruction);
        return null;
    }
}
