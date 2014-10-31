/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.mapping.MappingGroup;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.ExceptionMessages.ActionInterpretation;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.exception.UncheckedException;
import edu.ucsd.forward.fpl.FplInterpretationException;
import edu.ucsd.forward.fpl.ast.FunctionDefinition;
import edu.ucsd.forward.fpl.ast.ParameterDeclaration;
import edu.ucsd.forward.fpl.ast.VariableDeclaration;
import edu.ucsd.forward.query.EagerQueryResult;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.explain.ExplanationPrinter;
import edu.ucsd.forward.query.function.AbstractFunction;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.general.GeneralFunction;
import edu.ucsd.forward.query.function.general.GeneralFunctionCall;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Represents the function that consists of a list of instruction to execute.
 * 
 * @author Yupeng
 * 
 */
public class FplFunction extends AbstractFunction implements GeneralFunction, ExplanationPrinter
{
    @SuppressWarnings("unused")
    private static final Logger      log = Logger.getLogger(FplFunction.class);
    
    private List<Instruction>        m_instructions;
    
    private Map<String, SchemaTree>  m_parameter_declarations;
    
    private Map<String, SchemaTree>  m_variable_declarations;
    
    private boolean                  m_is_action = false;
    
    /**
     * Indicates the index of instructions with labels.
     */
    private Map<String, Instruction> m_index;
    
    /**
     * Constructs an empty function plan given a definition. The function plan initially has no instructions.
     * 
     * @param definition
     *            the function definition.
     */
    public FplFunction(FunctionDefinition definition)
    {
        super(definition.getName());
        
        assert definition != null;
        
        m_instructions = new LinkedList<Instruction>();
        m_index = new HashMap<String, Instruction>();
        
        // Build the function signature
        FunctionSignature signature;
        if (definition.getReturnType() != null) signature = new FunctionSignature(definition.getName(), definition.getReturnType());
        else signature = new FunctionSignature(definition.getName());
        for (ParameterDeclaration declaration : definition.getParameterDeclarations())
        {
            signature.addArgument(declaration.getName(), declaration.getType());
        }
        this.addFunctionSignature(signature);
        
        // Copy the parameter declarations for use during evaluation
        m_parameter_declarations = new LinkedHashMap<String, SchemaTree>();
        for (ParameterDeclaration param_decl : definition.getParameterDeclarations())
            m_parameter_declarations.put(param_decl.getName(), new SchemaTree(param_decl.getType()));
        
        // Copy the variable declarations for use during evaluation
        m_variable_declarations = new LinkedHashMap<String, SchemaTree>();
        if (definition.getDeclarationSection() != null)
        {
            for (VariableDeclaration var_decl : definition.getDeclarationSection().getVariableDeclarations())
                m_variable_declarations.put(var_decl.getName(), new SchemaTree(var_decl.getType()));
        }
    }
    
    /**
     * Constructs an empty function plan.
     * 
     * @param name
     *            the function name.
     * @param return_type
     *            the return type
     * @param para_decls
     *            the parameter declaration types
     * @param var_decls
     *            the variable declaration types
     */
    public FplFunction(String name, Type return_type, Map<String, SchemaTree> para_decls, Map<String, SchemaTree> var_decls)
    {
        super(name);
        
        assert name != null && para_decls != null && var_decls != null;
        m_instructions = new LinkedList<Instruction>();
        m_index = new HashMap<String, Instruction>();
        
        // Build the function signature
        FunctionSignature signature;
        if (return_type != null) signature = new FunctionSignature(name, return_type);
        else signature = new FunctionSignature(name);
        for (String para_name : para_decls.keySet())
        {
            signature.addArgument(para_name, para_decls.get(para_name).getRootType());
        }
        this.addFunctionSignature(signature);
        
        // Copy the parameter declarations for use during evaluation
        m_parameter_declarations = new LinkedHashMap<String, SchemaTree>(para_decls);
        
        // Copy the variable declarations for use during evaluation
        m_variable_declarations = new LinkedHashMap<String, SchemaTree>(var_decls);
    }

    /**
     * Sets whether this function is an action as well.
     * 
     * @param is_action
     *            whether this function is an action.
     */
    public void setIsAction(boolean is_action)
    {
        m_is_action = is_action;
    }
    
    /**
     * Gets the parameter declarations.
     * 
     * @return the parameter declarations
     */
    public Map<String, SchemaTree> getParameterDeclarations()
    {
        return m_parameter_declarations;
    }
    
    /**
     * Gets the variable declarations.
     * 
     * @return the variable declarations
     */
    public Map<String, SchemaTree> getVariableDeclarations()
    {
        return m_variable_declarations;
    }
    
    /**
     * Removes any instructions. This method is used right before recompiling the function.
     */
    public void clear()
    {
        m_index.clear();
        m_instructions.clear();
    }
    
    /**
     * Gets the instructions.
     * 
     * @return the instructions.
     */
    public List<Instruction> getInstructions()
    {
        return m_instructions;
    }
    
    /**
     * Adds one instruction.
     * 
     * @param instruction
     *            the instruction to add.
     */
    public void addInstruction(Instruction instruction)
    {
        assert instruction != null;
        m_instructions.add(instruction);
        for (String label : instruction.getLabels())
        {
            putIndex(label, instruction);
        }
    }
    
    /**
     * Puts to the index an entry of label to instruction.
     * 
     * @param label
     *            the label of the instruction.
     * @param instruction
     *            the instruction to be added to index.
     */
    private void putIndex(String label, Instruction instruction)
    {
        assert label != null && instruction != null;
        m_index.put(label, instruction);
    }
    
    /**
     * Gets the instruction that marked with the specified label.
     * 
     * @param label
     *            the label of the instruction.
     * @return the instruction that marked with the specified label.
     */
    public Instruction getInstruction(String label)
    {
        return m_index.get(label);
    }
    
    /**
     * Gets the next instruction in the sequence to the specified instruction.
     * 
     * @param instruction
     *            the instruction whose next instruction to get
     * @return the next instruction in the sequence to the specified instruction.
     */
    public Instruction next(Instruction instruction)
    {
        assert instruction != null;
        int idx = m_instructions.indexOf(instruction);
        // Reach the end of the list
        if (idx == m_instructions.size() - 1) return null;
        assert idx != -1;
        return m_instructions.get(idx + 1);
    }
    
    /**
     * Prepares the local data source for the function execution by creating schema objects for the declared variable and creating
     * schema/data objects for the input arguments.
     * 
     * @param arguments
     *            the input arguments to the function
     * @param uas
     *            the unified application state.
     * @throws QueryExecutionException
     *             if anything wrong occurs during the preparation.
     */
    private void before(List<Value> arguments, UnifiedApplicationState uas) throws QueryExecutionException
    {
        assert arguments != null;
        
        // Request a new frame from the FPL storage stack
        uas.requestNewFplSource();
        
        // Create schema tree and data tree for the parameters and the declared variables
        TupleType data_obj_type = new TupleType();
        TupleValue data_obj_value = new TupleValue();
        
        // Create a tuple value containing the parameters and the declared variables
        assert arguments.size() == m_parameter_declarations.size();
        int idx = 0;
        for (Map.Entry<String, SchemaTree> entry : m_parameter_declarations.entrySet())
        {
            data_obj_type.setAttribute(entry.getKey(), TypeUtil.cloneNoParent(entry.getValue()));
            data_obj_value.setAttribute(entry.getKey(), arguments.get(idx));
            idx++;
        }
        for (Map.Entry<String, SchemaTree> entry : m_variable_declarations.entrySet())
        {
            data_obj_type.setAttribute(entry.getKey(), TypeUtil.cloneNoParent(entry.getValue()));
            // Creates a dummy data object as default value
            data_obj_value.setAttribute(entry.getKey(), new NullValue());
        }
        
        // Create a temporary data object in the FPL local scope data source
        String schema_obj_name = this.getName();
        try
        {
            // Evaluate mapping query if it exists and the current function is an action
            // Only the action should have access to the context.
            if (!uas.getMappingGroups().isEmpty() && m_is_action)
            {
                // We can only have at most one mapping group at a time
                assert (uas.getMappingGroups().size() == 1);
                
                MappingGroup mapping_group = uas.getMappingGroups().get(0);
                
                // Restore the context schema
                uas.getDataSource(DataSource.FPL_LOCAL_SCOPE).createSchemaObject(DataSource.CONTEXT,
                                                                                 mapping_group.getContextSchema(), Size.SMALL);
                mapping_group.evaluateMappingQuery();
            }
            
            uas.getDataSource(DataSource.FPL_LOCAL_SCOPE).createSchemaObject(schema_obj_name, new SchemaTree(data_obj_type),
                                                                             Size.SMALL);
            uas.getDataSource(DataSource.FPL_LOCAL_SCOPE).setDataObject(schema_obj_name, new DataTree(data_obj_value));
        }
        catch (DataSourceException e)
        {
            // This should never happen
            throw new AssertionError();
        }
        
        // // Create the local data source
        // String data_source_name = this.getName();
        // DataSource data_source = new InMemoryDataSource(data_source_name, DataModel.SQLPLUSPLUS);
        // uas.addDataSource(data_source);
        //
        // // Create schema objects and data objects for the parameters
        // assert arguments.size() == m_parameter_declarations.size();
        // int idx = 0;
        // for (Map.Entry<String, SchemaTree> entry : m_parameter_declarations.entrySet())
        // {
        // String data_object_name = entry.getKey();
        // DataTree data_tree = new DataTree(arguments.get(idx));
        // uas.addSchemaAndData(data_source_name, data_object_name, entry.getValue(), Size.SMALL, data_tree);
        // idx++;
        // }
        //
        // // Create schema objects for declared variables
        // for (Map.Entry<String, SchemaTree> entry : m_variable_declarations.entrySet())
        // {
        // // Creates a dummy data object as default value
        // // DataTree data_tree = ValueUtil.initialize(entry.getValue());
        // data_source.createSchemaObject(entry.getKey(), entry.getValue(), Size.SMALL);
        // }
    }
    
    /**
     * Cleans after the function execution by removing the local data source.
     * 
     * @param uas
     *            the unified application state.
     */
    private void after(UnifiedApplicationState uas)
    {
        // Drop the data object in the FPL local scope data source
        String schema_obj_name = this.getName();
        try
        {
            if (uas.getDataSource(DataSource.FPL_LOCAL_SCOPE).hasSchemaObject(schema_obj_name))
            {
                uas.getDataSource(DataSource.FPL_LOCAL_SCOPE).dropSchemaObject(schema_obj_name);
            }
            // Remove the top frame in the fpl storage stack.
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
     * Runs the function plan.
     * 
     * @param arguments
     *            the arguments to the function.
     * @param uas
     *            the unified application state.
     * @return the return value.
     * @throws FplInterpretationException
     *             if error occurs during action interpretation.
     */
    private Value run(List<Value> arguments, UnifiedApplicationState uas) throws FplInterpretationException
    {
        // Prepare the local data source.
        before(arguments, uas);
        
        if (m_instructions.isEmpty()) return new NullValue();
        Instruction instruction = m_instructions.get(0);
        
        try
        {
            do
            {
                String label = instruction.execute(uas);
                if (label == null)
                {
                    // Proceed with the next instruction
                    instruction = next(instruction);
                }
                else if (label.equals(ReturnInstruction.RETURN_LABEL))
                {
                    PhysicalPlan return_plan = ((ReturnInstruction) instruction).getReturnPlan();
                    Value return_value = new NullValue();
                    if (return_plan != null)
                    {
                        // Executes the return plan
                        EagerQueryResult result = QueryProcessorFactory.getInstance().createEagerQueryResult(return_plan, uas);
                        return_value = result.getValue();
                    }
                    after(uas);
                    return return_value;
                }
                else
                {
                    if (!m_index.containsKey(label))
                    {
                        throw new FplInterpretationException(ActionInterpretation.NON_EXISTING_INSTRUCTION, label);
                    }
                    instruction = m_index.get(label);
                }
            } while (instruction != null);
            
            throw new FplInterpretationException(ActionInterpretation.NO_RETURN_INSTRUCTION);
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Cleanup
            this.after(uas);
            
            // Throw the expected exception
            if (t instanceof FplInterpretationException) throw (FplInterpretationException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    @Override
    public String toString()
    {
        return toExplainString();
    }
    
    @Override
    public String toExplainString()
    {
        String str = "";
        int idx = 0;
        for (Instruction instruction : m_instructions)
        {
            str += idx + instruction.toExplainString();
            idx++;
        }
        return str;
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return false;
    }
    
    @Override
    public BindingValue evaluate(GeneralFunctionCall call, Binding input) throws QueryExecutionException
    {
        UnifiedApplicationState uas = QueryProcessorFactory.getInstance().getUnifiedApplicationState();
        
        // Get the argument values
        List<Value> arguments = new ArrayList<Value>();
        for (Term term : call.getArguments())
        {
            BindingValue arg_binding_value = TermEvaluator.evaluate(term, input);
            Value arg_value = arg_binding_value.getValue();
            // Clone the value if it is not cloned.
            if (!arg_binding_value.isCloned()) arg_value = ValueUtil.cloneNoParentNoType(arg_value);
            arguments.add(arg_value);
        }
        // FIXME: We get the UAS from the query processor based on the assumption the UAS is locked by the invoker.
        try
        {
            Value return_value = run(arguments, uas);
            return new BindingValue(return_value, false);
        }
        catch (FplInterpretationException e)
        {
            throw new QueryExecutionException(QueryExecution.FPL_FUNCTION_ERROR, e, this.getName());
        }
    }
}
