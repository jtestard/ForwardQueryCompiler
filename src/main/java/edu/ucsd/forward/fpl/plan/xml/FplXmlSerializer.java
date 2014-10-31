/**
 * 
 */
package edu.ucsd.forward.fpl.plan.xml;

import java.util.Map;

import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.xml.TypeXmlSerializer;
import edu.ucsd.forward.fpl.plan.ActionCall;
import edu.ucsd.forward.fpl.plan.ConditionalJumpInstruction;
import edu.ucsd.forward.fpl.plan.DynamicActionCall;
import edu.ucsd.forward.fpl.plan.FplAction;
import edu.ucsd.forward.fpl.plan.FplFunction;
import edu.ucsd.forward.fpl.plan.Instruction;
import edu.ucsd.forward.fpl.plan.NextInstruction;
import edu.ucsd.forward.fpl.plan.QueryInstruction;
import edu.ucsd.forward.fpl.plan.ReturnInstruction;
import edu.ucsd.forward.fpl.plan.StaticActionCall;
import edu.ucsd.forward.fpl.plan.UnconditionalJumpInstruction;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.xml.PlanXmlSerializer;
import edu.ucsd.forward.xml.AbstractXmlSerializer;

/**
 * Utility class to serialize a fpl function/action to an XML DOM element.
 * 
 * @author Yupeng Fu
 * 
 */
public class FplXmlSerializer extends AbstractXmlSerializer
{
    @SuppressWarnings("unused")
    private static final Logger   log                 = Logger.getLogger(FplXmlSerializer.class);
    
    protected static final String FPL_FUNC_ELM        = "fpl_function";
    
    protected static final String PARA_DECLARE_ELM    = "parameter_declaration";
    
    protected static final String VAR_DECLARE_ELM     = "variable_declaration";
    
    protected static final String LABELS_ATTR         = "labels";
    
    protected static final String PLAN_ELM            = "physical_plan";
    
    public static final String    NAME_ATTR           = "name";
    
    protected static final String TARGET_ATTR         = "target";
    
    protected static final String EXPECTED_VALUE_ATTR = "expected_value";
    
    protected static final String TYPE_ATTR           = "type";
    
    protected static final String NEW_WINDOW_ATTR     = "new_window";
    
    protected static final String HTTP_VERB           = "http_verb";
    
    protected static final String ARGUMENTS_ELM       = "arguments";
    
    protected static final String ACTION_PATH_ATTR    = "action_path";
    
    protected static final String METHOD_ATTR         = "method";
    
    protected static final String INSTRUCTIONS_ELM    = "instructions";
    
    protected static final String FPL_ACTION_ELM      = "fpl_action";
    
    protected static final String INPUT_SCHEMA_ELM    = "input_schema";
    
    /**
     * Constructor.
     */
    public FplXmlSerializer()
    {
        
    }
    
    /**
     * Constructs with an existing document.
     * 
     * @param document
     *            the document.
     */
    public FplXmlSerializer(Document document)
    {
        super(document);
    }
    
    private Element serializeInstruction(Instruction instruction)
    {
        // Instruction XML element
        Element instruction_elm = getDomDocument().createElement(instruction.getName());
        
        // Add the labels
        String labels = "";
        boolean first = true;
        for (String label : instruction.getLabels())
        {
            if (first)
            {
                first = false;
            }
            else
            {
                labels += ";";
            }
            labels += label;
        }
        instruction_elm.setAttribute(LABELS_ATTR, labels);
        
        if (instruction.getClass() == QueryInstruction.class)
        {
            QueryInstruction query_instruction = (QueryInstruction) instruction;
            PlanXmlSerializer serializer = new PlanXmlSerializer(getDomDocument());
            Element plan_elm = serializer.serializePhysicalPlan(query_instruction.getQueryPlan());
            instruction_elm.appendChild(plan_elm);
        }
        else if (instruction.getClass() == ConditionalJumpInstruction.class)
        {
            ConditionalJumpInstruction cond_jump_instruction = (ConditionalJumpInstruction) instruction;
            PlanXmlSerializer serializer = new PlanXmlSerializer(getDomDocument());
            Element plan_elm = serializer.serializePhysicalPlan(cond_jump_instruction.getConditionPlan());
            instruction_elm.appendChild(plan_elm);
            instruction_elm.setAttribute(TARGET_ATTR, cond_jump_instruction.getTargetLabel());
            instruction_elm.setAttribute(EXPECTED_VALUE_ATTR, cond_jump_instruction.getExpectedValue() + "");
        }
        else if (instruction.getClass() == UnconditionalJumpInstruction.class)
        {
            UnconditionalJumpInstruction jump_instruction = (UnconditionalJumpInstruction) instruction;
            instruction_elm.setAttribute(TARGET_ATTR, jump_instruction.getTargetLabel());
        }
        else if (instruction.getClass() == ReturnInstruction.class)
        {
            ReturnInstruction return_instruction = (ReturnInstruction) instruction;
            PhysicalPlan return_plan = return_instruction.getReturnPlan();
            if (return_plan != null)
            {
                PlanXmlSerializer serializer = new PlanXmlSerializer(getDomDocument());
                Element plan_elm = serializer.serializePhysicalPlan(return_plan);
                instruction_elm.appendChild(plan_elm);
            }
        }
        else if (instruction.getClass() == StaticActionCall.class)
        {
            StaticActionCall static_call = (StaticActionCall) instruction;
            serializeStaticActionCall(static_call, instruction_elm);
        }
        else if (instruction.getClass() == DynamicActionCall.class)
        {
            DynamicActionCall dynamic_call = (DynamicActionCall) instruction;
            serializeActionCall(dynamic_call, instruction_elm);
            // serialize action path
            PlanXmlSerializer serializer = new PlanXmlSerializer(getDomDocument());
            Element argu_elm = getDomDocument().createElement(ACTION_PATH_ATTR);
            Element plan_elm = serializer.serializePhysicalPlan(dynamic_call.getActionPath());
            instruction_elm.appendChild(argu_elm);
            argu_elm.appendChild(plan_elm);
        }
        else
        {
            assert instruction instanceof NextInstruction;
        }
        
        return instruction_elm;
    }
    
    public void serializeStaticActionCall(StaticActionCall call, Element instruction_elm)
    {
        serializeActionCall(call, instruction_elm);
        instruction_elm.setAttribute(METHOD_ATTR, call.getMethod().toString());
        instruction_elm.setAttribute(ACTION_PATH_ATTR, call.getActionPath());
    }
    
    private void serializeActionCall(ActionCall action_call, Element instruction_elm)
    {
        instruction_elm.setAttribute(NEW_WINDOW_ATTR, action_call.inNewWindow() + "");
        instruction_elm.setAttribute(HTTP_VERB, action_call.getHttpVerb().toString());
        
        Element argu_elm = getDomDocument().createElement(ARGUMENTS_ELM);
        instruction_elm.appendChild(argu_elm);
        for (PhysicalPlan plan : action_call.getCallArguments())
        {
            PlanXmlSerializer serializer = new PlanXmlSerializer(getDomDocument());
            Element plan_elm = serializer.serializePhysicalPlan(plan);
            argu_elm.appendChild(plan_elm);
        }
    }
    
    /**
     * Converts a fpl action into the XML document representation.
     * 
     * @param action
     *            the fpl action
     * @return the root element of the converted XML document
     */
    public Element serializeFplAction(FplAction action)
    {
        Element root = getDomDocument().createElement(FPL_ACTION_ELM);
        
        root.setAttribute(ACTION_PATH_ATTR, action.getPath());
        root.setAttribute(NAME_ATTR, action.getFunctionName());
        PlanXmlSerializer serializer = new PlanXmlSerializer(getDomDocument());
        Element plan_elm = serializer.serializePhysicalPlan(action.getPlan());
        root.appendChild(plan_elm);
        return root;
    }
    
    /**
     * Converts a fpl function into the XML document representation.
     * 
     * @param function
     *            the fpl function
     * @return the root element of the converted XML document
     */
    public Element serializeFplFunction(FplFunction function)
    {
        Element root = getDomDocument().createElement(FPL_FUNC_ELM);
        
        // Serialize parameter declarations
        Map<String, SchemaTree> para_declarations = function.getParameterDeclarations();
        for (String name : para_declarations.keySet())
        {
            Element declaration_elm = getDomDocument().createElement(PARA_DECLARE_ELM);
            TypeXmlSerializer.serializeSchemaTree(para_declarations.get(name), declaration_elm);
            declaration_elm.setAttribute(NAME_ATTR, name);
            root.appendChild(declaration_elm);
        }
        
        // Serialize variable declarations
        Map<String, SchemaTree> var_declarations = function.getVariableDeclarations();
        for (String name : var_declarations.keySet())
        {
            Element declaration_elm = getDomDocument().createElement(VAR_DECLARE_ELM);
            TypeXmlSerializer.serializeSchemaTree(var_declarations.get(name), declaration_elm);
            declaration_elm.setAttribute(NAME_ATTR, name);
            root.appendChild(declaration_elm);
        }
        
        // Serialize return type
        Type return_type = function.getFunctionSignatures().get(0).getReturnType();
        if (return_type != null)
        {
            TypeXmlSerializer.serializeType(return_type, TYPE_ATTR, root);
        }
        
        Element instructions_elm = getDomDocument().createElement(INSTRUCTIONS_ELM);
        
        // Serialize instructions
        for (Instruction instruction : function.getInstructions())
        {
            instructions_elm.appendChild(serializeInstruction(instruction));
        }
        root.appendChild(instructions_elm);
        
        // Set the name attribute
        root.setAttribute(NAME_ATTR, function.getName());
        
        return root;
    }
    
}
