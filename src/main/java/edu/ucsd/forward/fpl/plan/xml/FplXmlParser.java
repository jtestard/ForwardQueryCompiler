/**
 * 
 */
package edu.ucsd.forward.fpl.plan.xml;

import static edu.ucsd.forward.xml.XmlUtil.getAttribute;
import static edu.ucsd.forward.xml.XmlUtil.getChildElements;
import static edu.ucsd.forward.xml.XmlUtil.getNonEmptyAttribute;
import static edu.ucsd.forward.xml.XmlUtil.getOnlyChildElement;
import static edu.ucsd.forward.xml.XmlUtil.getOptionalChildElement;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.SystemUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.TypeXmlSerializer;
import edu.ucsd.forward.fpl.ast.ActionInvocation.HttpVerb;
import edu.ucsd.forward.fpl.ast.ActionInvocation.Method;
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
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.xml.PlanXmlParser;
import edu.ucsd.forward.query.xml.PlanXmlSerializer;
import edu.ucsd.forward.xml.AbstractXmlParser;
import edu.ucsd.forward.xml.XmlParserException;

/**
 * Utility class to parse a fpl function from an XML DOM element.
 * 
 * @author Yupeng Fu
 * 
 */
public final class FplXmlParser extends AbstractXmlParser
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(FplXmlParser.class);
    
    /**
     * Constructor.
     */
    public FplXmlParser()
    {
        
    }
    
    /**
     * Prepares the local data source for the function compilation by creating schema objects for the declared variable and the
     * parameters.
     */
    private static void before(UnifiedApplicationState uas, String name, Map<String, SchemaTree> para_decls,
            Map<String, SchemaTree> var_decls)
    {
        // Request a new frame in storage stack
        try
        {
            uas.requestNewFplSource();
        }
        catch (QueryExecutionException e1)
        {
            // This should never happen
            throw new AssertionError();
        }
        
        // Create schema tree and data tree for the parameters and the declared variables
        TupleType data_obj_type = new TupleType();
        
        // Create a tuple value containing the parameters and the declared variables
        for (String para_name : para_decls.keySet())
        {
            data_obj_type.setAttribute(para_name, TypeUtil.cloneNoParent(para_decls.get(para_name).getRootType()));
        }
        
        for (String var_name : var_decls.keySet())
        {
            data_obj_type.setAttribute(var_name, TypeUtil.cloneNoParent(var_decls.get(var_name).getRootType()));
        }
        
        try
        {
            // Create a schema object in the FPL local scope data source
            String schema_obj_name = name;
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
     * Cleans after the function compilation by removing the local data source.
     * 
     */
    private static void after(UnifiedApplicationState uas, String name)
    {
        // Drop the schema object in the FPL local scope data source
        try
        {
            if (uas.getDataSource(DataSource.FPL_LOCAL_SCOPE).hasSchemaObject(name))
            {
                uas.getDataSource(DataSource.FPL_LOCAL_SCOPE).dropSchemaObject(name);
            }
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
    
    private static Instruction parseInstruction(Element instruction_elm, UnifiedApplicationState uas) throws XmlParserException
    {
        String name = instruction_elm.getTagName();
        Instruction instruction = null;
        if (compareName(name, QueryInstruction.class))
        {
            PlanXmlParser parser = new PlanXmlParser();
            Element plan_elm = getOnlyChildElement(instruction_elm);
            PhysicalPlan plan = parser.parsePhysicalPlan(uas, plan_elm);
            instruction = new QueryInstruction(plan);
        }
        else if (compareName(name, ConditionalJumpInstruction.class))
        {
            boolean expected_value = Boolean.parseBoolean(getNonEmptyAttribute(instruction_elm,
                                                                               FplXmlSerializer.EXPECTED_VALUE_ATTR));
            String target_label = getNonEmptyAttribute(instruction_elm, FplXmlSerializer.TARGET_ATTR);
            Element plan_elm = getOnlyChildElement(instruction_elm);
            PlanXmlParser parser = new PlanXmlParser();
            PhysicalPlan plan = parser.parsePhysicalPlan(uas, plan_elm);
            instruction = new ConditionalJumpInstruction(expected_value, plan);
            ((ConditionalJumpInstruction) instruction).setTargetLabel(target_label);
        }
        else if (compareName(name, UnconditionalJumpInstruction.class))
        {
            String target_label = getNonEmptyAttribute(instruction_elm, FplXmlSerializer.TARGET_ATTR);
            instruction = new UnconditionalJumpInstruction(target_label);
        }
        else if (compareName(name, ReturnInstruction.class))
        {
            Element child_elm = getOptionalChildElement(instruction_elm);
            instruction = new ReturnInstruction();
            if (child_elm != null)
            {
                PlanXmlParser parser = new PlanXmlParser();
                PhysicalPlan return_plan = parser.parsePhysicalPlan(uas, child_elm);
                ((ReturnInstruction) instruction).setQueryPlan(return_plan);
            }
        }
        else if (compareName(name, StaticActionCall.class))
        {
            instruction = parseStaticActionCall(uas, instruction_elm);
        }
        else if (compareName(name, DynamicActionCall.class))
        {
            boolean new_window = Boolean.parseBoolean(getNonEmptyAttribute(instruction_elm, FplXmlSerializer.NEW_WINDOW_ATTR));
            HttpVerb verb = HttpVerb.valueOf(getNonEmptyAttribute(instruction_elm, FplXmlSerializer.HTTP_VERB));
            
            List<PhysicalPlan> arguments = new ArrayList<PhysicalPlan>();
            Element argu_elm = getOnlyChildElement(instruction_elm, FplXmlSerializer.ARGUMENTS_ELM);
            for (Element plan_elm : getChildElements(argu_elm))
            {
                PlanXmlParser parser = new PlanXmlParser();
                arguments.add(parser.parsePhysicalPlan(uas, plan_elm));
            }
            
            Element action_path_elm = getOnlyChildElement(instruction_elm, FplXmlSerializer.ACTION_PATH_ATTR);
            Element plan_elm = getOnlyChildElement(action_path_elm);
            PlanXmlParser parser = new PlanXmlParser();
            PhysicalPlan action_path = parser.parsePhysicalPlan(uas, plan_elm);
            
            instruction = new DynamicActionCall(verb, new_window, action_path, arguments);
        }
        else
        {
            instruction = new NextInstruction();
        }
        
        // Parse labels
        String labels = getAttribute(instruction_elm, FplXmlSerializer.LABELS_ATTR);
        for (String label : labels.split(";"))
        {
            instruction.addLabel(label);
        }
        
        return instruction;
    }
    
    public static StaticActionCall parseStaticActionCall(UnifiedApplicationState uas, Element instruction_elm)
            throws XmlParserException
    {
        boolean new_window = Boolean.parseBoolean(getNonEmptyAttribute(instruction_elm, FplXmlSerializer.NEW_WINDOW_ATTR));
        HttpVerb verb = HttpVerb.valueOf(getNonEmptyAttribute(instruction_elm, FplXmlSerializer.HTTP_VERB));
        String action_path = getNonEmptyAttribute(instruction_elm, FplXmlSerializer.ACTION_PATH_ATTR);
        
        List<PhysicalPlan> arguments = new ArrayList<PhysicalPlan>();
        Element argu_elm = getOnlyChildElement(instruction_elm, FplXmlSerializer.ARGUMENTS_ELM);
        for (Element plan_elm : getChildElements(argu_elm))
        {
            PlanXmlParser parser = new PlanXmlParser();
            arguments.add(parser.parsePhysicalPlan(uas, plan_elm));
        }
        Method method = Method.valueOf(getNonEmptyAttribute(instruction_elm, FplXmlSerializer.METHOD_ATTR));
        return new StaticActionCall(verb, method, new_window, action_path, arguments);
    }
    
    /**
     * Compares if the given tag name equals the name of the given class. The comparison is case insensitive and removes the
     * underscores from the tag name.
     * 
     * @param tag_name
     *            the tag name to be compared.
     * @param clazz
     *            the class to compare.
     * @return <code>true</code> if the given name equals the name of the given class; <code>false</code> otherwise.
     */
    private static boolean compareName(String tag_name, Class<? extends Object> clazz)
    {
        String expected = SystemUtil.getSimpleName(clazz);
        
        // Remove underscores from tag name
        String new_name = tag_name.replaceAll("_", "");
        
        return new_name.equalsIgnoreCase(expected);
    }
    
    /**
     * Builds the fpl function definition from the XML input without parsing the instructions.
     * 
     * @param uas
     *            the unified application state that may be used by the plan.
     * @param fpl_func_elm
     *            the fpl function element of the XML DOM.
     * @return the built physical query plan.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    public static FplFunction parseFplFunctionDefinition(UnifiedApplicationState uas, Element fpl_func_elm)
            throws XmlParserException
    {
        String name = getNonEmptyAttribute(fpl_func_elm, FplXmlSerializer.NAME_ATTR);
        // Parse parameter declarations
        Map<String, SchemaTree> para_decls = new LinkedHashMap<String, SchemaTree>();
        for (Element para_declare_elm : getChildElements(fpl_func_elm, FplXmlSerializer.PARA_DECLARE_ELM))
        {
            String para_name = getNonEmptyAttribute(para_declare_elm, FplXmlSerializer.NAME_ATTR);
            Element schema_tree_elm = getOnlyChildElement(para_declare_elm, TypeXmlSerializer.SCHEMA_TREE_ELM);
            SchemaTree schema_tree = TypeXmlParser.parseSchemaTree(schema_tree_elm);
            para_decls.put(para_name, schema_tree);
        }
        // Parse variable declarations
        Map<String, SchemaTree> var_decls = new LinkedHashMap<String, SchemaTree>();
        for (Element var_declare_elm : getChildElements(fpl_func_elm, FplXmlSerializer.VAR_DECLARE_ELM))
        {
            String var_name = getNonEmptyAttribute(var_declare_elm, FplXmlSerializer.NAME_ATTR);
            Element schema_tree_elm = getOnlyChildElement(var_declare_elm, TypeXmlSerializer.SCHEMA_TREE_ELM);
            SchemaTree schema_tree = TypeXmlParser.parseSchemaTree(schema_tree_elm);
            var_decls.put(var_name, schema_tree);
        }
        // Parse return type
        Type return_type = null;
        Element return_type_elm = getOptionalChildElement(fpl_func_elm, FplXmlSerializer.TYPE_ATTR);
        if (return_type_elm != null) return_type = TypeXmlParser.parseType(return_type_elm);
        
        FplFunction fpl_function = new FplFunction(name, return_type, para_decls, var_decls);
        
        return fpl_function;
    }
    
    /**
     * Parses the instructions of given FPL function.
     * 
     * @param uas
     *            the unified application state that may be used by the function.
     * @param fpl_func_elm
     *            the fpl function element of the XML DOM.
     * @param function
     *            the function definition.
     * @throws XmlParserException
     *             when the instruction parsing fails
     */
    public static void parseFplFunctionImpl(UnifiedApplicationState uas, Element fpl_func_elm, FplFunction function)
            throws XmlParserException
    {
        assert function != null;
        String name = function.getName();
        String func_name = getNonEmptyAttribute(fpl_func_elm, FplXmlSerializer.NAME_ATTR);
        assert name.equalsIgnoreCase(func_name);
        
        before(uas, name, function.getParameterDeclarations(), function.getVariableDeclarations());
        
        try
        {
            // Parse instructions
            Element instructions_elm = getOnlyChildElement(fpl_func_elm, FplXmlSerializer.INSTRUCTIONS_ELM);
            for (Element instruction_elm : getChildElements(instructions_elm))
            {
                Instruction instruction = parseInstruction(instruction_elm, uas);
                function.addInstruction(instruction);
            }
        }
        catch (XmlParserException exception)
        {
            after(uas, name);
            throw exception;
        }
        
        after(uas, name);
    }
    
    /**
     * Builds the fpl function from the XML input.
     * 
     * @param uas
     *            the unified application state that may be used by the plan.
     * @param fpl_func_elm
     *            the fpl function element of the XML DOM.
     * @return the built fpl function.
     * @throws XmlParserException
     *             when the data source is not accessible.
     */
    public static FplFunction parseFplFunction(UnifiedApplicationState uas, Element fpl_func_elm) throws XmlParserException
    {
        FplFunction fpl_function = parseFplFunctionDefinition(uas, fpl_func_elm);
        parseFplFunctionImpl(uas, fpl_func_elm, fpl_function);
        return fpl_function;
    }
    
    /**
     * Parses a fpl action from the XML input.
     * 
     * @param uas
     *            the unified application state that may be used by the plan.
     * @param fpl_action_elm
     *            the fpl action element of the XML DOM.
     * @return the parsed fpl action.
     * @throws XmlParserException
     *             anything wrong happens
     */
    public static FplAction parseFplAction(UnifiedApplicationState uas, Element fpl_action_elm) throws XmlParserException
    {
        String action_path = getNonEmptyAttribute(fpl_action_elm, FplXmlSerializer.ACTION_PATH_ATTR);
        String func_name = getNonEmptyAttribute(fpl_action_elm, FplXmlSerializer.NAME_ATTR);
        
        PlanXmlParser parser = new PlanXmlParser();
        Element plan_elm = getOnlyChildElement(fpl_action_elm, PlanXmlSerializer.QUERY_PLAN_ELM);
        PhysicalPlan plan = parser.parsePhysicalPlan(uas, plan_elm);
        
        FplAction action = new FplAction(action_path, func_name, plan);
        return action;
    }
}
