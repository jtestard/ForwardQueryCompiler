/**
 * 
 */
package edu.ucsd.forward.data;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.testng.annotations.Test;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.SwitchType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.SwitchValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.ValueXmlParser;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.util.tree.TreePath.PathMode;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Tests schema paths.
 * 
 * @author Kian Win Ong
 * 
 */
@Test
public class TestSchemaPath extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestSchemaPath.class);
    
    /**
     * Tests parsing a schema path.
     * 
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    public void testParse() throws ParseException
    {
        checkParse("/", new String[] {}, PathMode.ABSOLUTE);
        checkParse("", new String[] {}, PathMode.RELATIVE);
        checkParse("/abc", new String[] { "abc" }, PathMode.ABSOLUTE);
        checkParse("abc", new String[] { "abc" }, PathMode.RELATIVE);
    }
    
    /**
     * Tests finding a type in a schema tree.
     * 
     * @throws ParseException
     *             if an error occurs during parsing.
     * @throws XmlParserException
     *             exception.
     */
    public void testFindType() throws ParseException, XmlParserException
    {
        String source = "TestSchemaPath-testFind.xml";
        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        SchemaTree schema_tree = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        
        // Get respective types by manually navigating the schema tree
        TupleType result = (TupleType) schema_tree.getRootType();
        StringType user_id = (StringType) result.getAttribute("user_id");
        CollectionType applications = (CollectionType) result.getAttribute("applications");
        TupleType application = (TupleType) applications.getTupleType();
        IntegerType application_id = (IntegerType) application.getAttribute("application_id");
        TupleType name = (TupleType) application.getAttribute("name");
        StringType first_name = (StringType) name.getAttribute("first_name");
        StringType last_name = (StringType) name.getAttribute("last_name");
        SwitchType applying_for = (SwitchType) application.getAttribute("applying_for");
        TupleType phd = (TupleType) applying_for.getCase("phd");
        StringType advisor_choice = (StringType) phd.getAttribute("advisor_choice");
        TupleType master = (TupleType) applying_for.getCase("master");
        StringType area_of_interest = (StringType) master.getAttribute("area_of_interest");
        CollectionType degrees = (CollectionType) application.getAttribute("degrees");
        TupleType degree = (TupleType) degrees.getTupleType();
        IntegerType degree_id = (IntegerType) degree.getAttribute("degree_id");
        
        // Check each type with its corresponding absolute path
        checkFindTypeAbsolute(result, "/");
        checkFindTypeAbsolute(user_id, "/user_id");
        checkFindTypeAbsolute(applications, "/applications");
        checkFindTypeAbsolute(application, "/applications/tuple");
        checkFindTypeAbsolute(application_id, "/applications/tuple/application_id");
        checkFindTypeAbsolute(name, "/applications/tuple/name");
        checkFindTypeAbsolute(first_name, "/applications/tuple/name/first_name");
        checkFindTypeAbsolute(last_name, "/applications/tuple/name/last_name");
        checkFindTypeAbsolute(applying_for, "/applications/tuple/applying_for");
        checkFindTypeAbsolute(phd, "/applications/tuple/applying_for/phd");
        checkFindTypeAbsolute(advisor_choice, "/applications/tuple/applying_for/phd/advisor_choice");
        checkFindTypeAbsolute(master, "/applications/tuple/applying_for/master");
        checkFindTypeAbsolute(area_of_interest, "/applications/tuple/applying_for/master/area_of_interest");
        checkFindTypeAbsolute(degrees, "/applications/tuple/degrees");
        checkFindTypeAbsolute(degree, "/applications/tuple/degrees/tuple");
        checkFindTypeAbsolute(degree_id, "/applications/tuple/degrees/tuple/degree_id");
        
        // Check each type with its corresponding relative path (from the "applications" collection type)
        checkFindTypeRelative(applications, applications, "");
        checkFindTypeRelative(applications, application, "tuple");
        checkFindTypeRelative(applications, application_id, "tuple/application_id");
        checkFindTypeRelative(applications, name, "tuple/name");
        checkFindTypeRelative(applications, first_name, "tuple/name/first_name");
        checkFindTypeRelative(applications, last_name, "tuple/name/last_name");
        checkFindTypeRelative(applications, applying_for, "tuple/applying_for");
        checkFindTypeRelative(applications, phd, "tuple/applying_for/phd");
        checkFindTypeRelative(applications, advisor_choice, "tuple/applying_for/phd/advisor_choice");
        checkFindTypeRelative(applications, master, "tuple/applying_for/master");
        checkFindTypeRelative(applications, area_of_interest, "tuple/applying_for/master/area_of_interest");
        checkFindTypeRelative(applications, degrees, "tuple/degrees");
        checkFindTypeRelative(applications, degree, "tuple/degrees/tuple");
        checkFindTypeRelative(applications, degree_id, "tuple/degrees/tuple/degree_id");
    }
    
    /**
     * Checks that a path is correctly parsed.
     * 
     * @param path_string
     *            the path in string format.
     * @param path_steps
     *            the path steps in string format.
     * @param path_mode
     *            the path mode.
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    private static void checkParse(String path_string, String[] path_steps, PathMode path_mode) throws ParseException
    {
        assert (path_string != null);
        assert (path_steps != null);
        assert (path_mode != null);
        
        SchemaPath path = new SchemaPath(path_string);
        
        assertEquals(Arrays.asList(path_steps), path.getPathSteps());
        assertEquals(path_mode, path.getPathMode());
        assertEquals(path_string, path.toString());
    }
    
    /**
     * Checks that an absolute path can be used to find a type in the schema tree.
     * 
     * @param type
     *            the expected type.
     * @param path_string
     *            the path in string format.
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    private static void checkFindTypeAbsolute(Type type, String path_string) throws ParseException
    {
        assert (type != null);
        assert (path_string != null);
        assert (path_string.startsWith("/"));
        
        SchemaTree schema_tree = (SchemaTree) type.getRoot();
        
        // Check that constructing a path from a string is equivalent to constructing one for a type node
        assertEquals(new SchemaPath(path_string), new SchemaPath(type));
        
        // Check that the path can be used to find the type again in the schema tree
        assertEquals(type, new SchemaPath(path_string).find(schema_tree));
        assertEquals(type, new SchemaPath(path_string).find(schema_tree.getRootType()));
    }
    
    /**
     * Checks that a relative path can be used to find a type in the schema tree.
     * 
     * @param start_type
     *            the type to start finding from.
     * @param type
     *            the expected type.
     * @param path_string
     *            the path in string format.
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    private static void checkFindTypeRelative(Type start_type, Type type, String path_string) throws ParseException
    {
        assert (type != null);
        assert (path_string != null);
        assert (start_type != null);
        assert (!path_string.startsWith("/"));
        
        // Check that constructing a path from a string is equivalent to constructing one for a type node
        assertEquals(new SchemaPath(path_string), new SchemaPath(start_type, type));
        
        // Check that the path can be used to find the type again in the schema tree
        assertEquals(type, new SchemaPath(path_string).find(start_type));
    }
    
    /**
     * Tests finding a list of values in a data tree.
     * 
     * @throws ParseException
     *             if an error occurs during parsing.
     * @throws XmlParserException
     *             exception.
     */
    public void testFindValues() throws ParseException, XmlParserException
    {
        String source = "TestSchemaPath-testFind.xml";
        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        SchemaTree source_schema = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        DataTree data_tree = ValueXmlParser.parseDataTree(XmlUtil.getOnlyChildElement(source_elem, "data_tree"), source_schema);
        
        // Get respective values by manually navigating the data tree
        TupleValue result = (TupleValue) data_tree.getRootValue();
        StringValue user_id = (StringValue) result.getAttribute("user_id");
        CollectionValue applications = (CollectionValue) result.getAttribute("applications");
        TupleValue application = (TupleValue) applications.getTuples().get(0);
        IntegerValue application_id = (IntegerValue) application.getAttribute("application_id");
        TupleValue name = (TupleValue) application.getAttribute("name");
        SwitchValue applying_for = (SwitchValue) application.getAttribute("applying_for");
        TupleValue phd = (TupleValue) applying_for.getCase();
        StringValue advisor_choice = (StringValue) phd.getAttribute("advisor_choice");
        CollectionValue degrees = (CollectionValue) application.getAttribute("degrees");
        TupleValue degree_1 = (TupleValue) degrees.getTuples().get(0);
        IntegerValue degree_1_id = (IntegerValue) degree_1.getAttribute("degree_id");
        StringValue degree_1_school = (StringValue) degree_1.getAttribute("school");
        TupleValue degree_3 = (TupleValue) degrees.getTuples().get(1);
        IntegerValue degree_3_id = (IntegerValue) degree_3.getAttribute("degree_id");
        StringValue degree_3_school = (StringValue) degree_3.getAttribute("school");
        
        List<Value> values = new ArrayList<Value>();
        
        // Check each value with its corresponding absolute path
        checkFindValuesAbsolute(Collections.<Value> singletonList(result), "/");
        checkFindValuesAbsolute(Collections.<Value> singletonList(user_id), "/user_id");
        checkFindValuesAbsolute(Collections.<Value> singletonList(applications), "/applications");
        checkFindValuesAbsolute(Collections.<Value> singletonList(application), "/applications/tuple");
        checkFindValuesAbsolute(Collections.<Value> singletonList(application_id), "/applications/tuple/application_id");
        checkFindValuesAbsolute(Collections.<Value> singletonList(name), "/applications/tuple/name");
        checkFindValuesAbsolute(Collections.<Value> singletonList(phd), "/applications/tuple/applying_for/phd");
        checkFindValuesAbsolute(Collections.<Value> singletonList(advisor_choice),
                                "/applications/tuple/applying_for/phd/advisor_choice");
        
        values.clear();
        values.add(degree_1);
        values.add(degree_3);
        checkFindValuesAbsolute(values, "/applications/tuple/degrees/tuple");
        
        values.clear();
        values.add(degree_1_id);
        values.add(degree_3_id);
        checkFindValuesAbsolute(values, "/applications/tuple/degrees/tuple/degree_id");
        
        values.clear();
        values.add(degree_1_school);
        values.add(degree_3_school);
        checkFindValuesAbsolute(values, "/applications/tuple/degrees/tuple/school");
        
        // Check each value with its corresponding relative path (from the "applications" collection value)
        checkFindValuesRelative(applications, Collections.<Value> singletonList(applications), "");
        checkFindValuesRelative(applications, Collections.<Value> singletonList(application), "tuple");
        checkFindValuesRelative(applications, Collections.<Value> singletonList(application_id), "tuple/application_id");
        checkFindValuesRelative(applications, Collections.<Value> singletonList(name), "tuple/name");
        checkFindValuesRelative(applications, Collections.<Value> singletonList(phd), "tuple/applying_for/phd");
        checkFindValuesRelative(applications, Collections.<Value> singletonList(advisor_choice),
                                "tuple/applying_for/phd/advisor_choice");
        
        values.clear();
        values.add(degree_1);
        values.add(degree_3);
        checkFindValuesRelative(applications, values, "tuple/degrees/tuple");
        
        values.clear();
        values.add(degree_1_id);
        values.add(degree_3_id);
        checkFindValuesRelative(applications, values, "tuple/degrees/tuple/degree_id");
        
        values.clear();
        values.add(degree_1_school);
        values.add(degree_3_school);
        checkFindValuesRelative(applications, values, "tuple/degrees/tuple/school");
    }
    
    /**
     * Checks that an absolute path can be used to find a list of values in the data tree.
     * 
     * @param values
     *            the list of values.
     * @param path_string
     *            the path in string format.
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    private static void checkFindValuesAbsolute(List<Value> values, String path_string) throws ParseException
    {
        assert (values != null);
        assert (path_string != null);
        assert (path_string.startsWith("/"));
        
        DataTree data_tree = (DataTree) values.get(0).getRoot();
        
        // Check that the path can be used to find the value again in the data tree
        assertEquals(values, new SchemaPath(path_string).find(data_tree));
        assertEquals(values, new SchemaPath(path_string).find(data_tree.getRootValue()));
    }
    
    /**
     * Checks that a relative path can be used to find a list of values in the data tree.
     * 
     * @param start_value
     *            the value to start finding from.
     * @param values
     *            the list of values.
     * @param path_string
     *            the path in string format.
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    private static void checkFindValuesRelative(Value start_value, List<Value> values, String path_string) throws ParseException
    {
        assert (values != null);
        assert (path_string != null);
        assert (!path_string.startsWith("/"));
        
        // Check that the path can be used to find the value again in the data tree
        assertEquals(values, new SchemaPath(path_string).find(start_value));
    }
    
}
