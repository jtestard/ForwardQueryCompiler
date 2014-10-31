/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.source.implicit.RequestDataSource;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.NullType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.fpl.FplInterpretationException;
import edu.ucsd.forward.fpl.ast.ActionInvocation.HttpVerb;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * Represents an action call in an FPL action.
 * 
 * @author Michalis Petropoulos
 * 
 */
public abstract class AbstractActionCall extends AbstractInstruction implements ActionCall
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractActionCall.class);
    
    private HttpVerb            m_verb;
    
    private boolean             m_new_window;
    
    private List<PhysicalPlan>  m_arguments;
    
    /**
     * The default constructor.
     * 
     * @param verb
     *            an HTTP verb.
     * @param new_window
     *            whether the action result will be displayed in a new browser window.
     * @param arguments
     *            the action call arguments.
     */
    public AbstractActionCall(HttpVerb verb, boolean new_window, List<PhysicalPlan> arguments)
    {
        assert (verb != null && arguments != null);
        m_verb = verb;
        m_new_window = new_window;
        m_arguments = new ArrayList<PhysicalPlan>(arguments);
    }
    
    @Override
    public HttpVerb getHttpVerb()
    {
        return m_verb;
    }
    
    @Override
    public boolean inNewWindow()
    {
        return m_new_window;
    }
    
    @Override
    public List<PhysicalPlan> getCallArguments()
    {
        return Collections.unmodifiableList(m_arguments);
    }
    
    /**
     * Executes the action call.
     * 
     * @param action_path
     *            the action path.
     * @param uas
     *            the unified application state.
     * @throws FplInterpretationException
     *             any error occurs during action interpretation.
     */
    public void execute(String action_path, UnifiedApplicationState uas) throws FplInterpretationException
    {
        TupleValue target_input_value = null;
        
        // HACK: Disabled because query processor cannot access application in visual layer
        /*-
        if (uas.getApplication().hasAction(action_path))
        {
            FplAction action = uas.getApplication().getAction(action_path);
            
            // Evaluate the arguments
            if (this.getCallArguments().size() > 0)
            {
                target_input_value = new TupleValue();
                List<Value> argument_values = new ArrayList<Value>();
                for (PhysicalPlan arg : this.getCallArguments())
                {
                    Value arg_value = QueryProcessorFactory.getInstance().createEagerQueryResult(arg, uas).getValue();
                    argument_values.add(arg_value);
                }
            }
        }
        */
        
        // Retrieve the request data source
        RequestDataSource rds = null;
        try
        {
            rds = (RequestDataSource) uas.getDataSource(RequestDataSource.NAME);
        }
        catch (DataSourceException e)
        {
            // this should never happen
            throw new AssertionError();
        }
        
        // Add and set the redirect target data object
        if (!rds.hasSchemaObject(RequestDataSource.REDIRECT_TARGET_DATA_OBJECT))
        {
            rds.createSchemaObject(RequestDataSource.REDIRECT_TARGET_DATA_OBJECT, new SchemaTree(new StringType()), Size.ONE);
        }
        rds.setDataObject(RequestDataSource.REDIRECT_TARGET_DATA_OBJECT, new DataTree(new StringValue(action_path)));
        
        // Add and set the redirect target input data object
        if (target_input_value != null)
        {
            if (!rds.hasSchemaObject(RequestDataSource.REDIRECT_TARGET_INPUT_DATA_OBJECT))
            {
                // FIXME Add type info
                rds.createSchemaObject(RequestDataSource.REDIRECT_TARGET_INPUT_DATA_OBJECT, new SchemaTree(new NullType()),
                                       Size.ONE);
            }
            rds.setDataObject(RequestDataSource.REDIRECT_TARGET_INPUT_DATA_OBJECT, new DataTree(target_input_value));
        }
        
        // Add and set the redirect new window data object
        if (!rds.hasSchemaObject(RequestDataSource.REDIRECT_NEW_WINDOW_DATA_OBJECT))
        {
            rds.createSchemaObject(RequestDataSource.REDIRECT_NEW_WINDOW_DATA_OBJECT, new SchemaTree(new BooleanType()), Size.ONE);
        }
        rds.setDataObject(RequestDataSource.REDIRECT_NEW_WINDOW_DATA_OBJECT, new DataTree(new BooleanValue(this.inNewWindow())));
        
        // Add and set the redirect method data object
        if (!rds.hasSchemaObject(RequestDataSource.REDIRECT_METHOD_DATA_OBJECT))
        {
            rds.createSchemaObject(RequestDataSource.REDIRECT_METHOD_DATA_OBJECT, new SchemaTree(new StringType()), Size.ONE);
        }
        rds.setDataObject(RequestDataSource.REDIRECT_METHOD_DATA_OBJECT, new DataTree(new StringValue(this.getHttpVerb().name())));
    }
    
}
