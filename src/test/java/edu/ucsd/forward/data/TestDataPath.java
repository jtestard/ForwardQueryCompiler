/**
 * 
 */
package edu.ucsd.forward.data;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.constraint.PrimaryKeyUtil;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.ScalarValue;
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
 * Tests data paths.
 * 
 * @author Kian Win Ong
 * 
 */
@Test
public class TestDataPath extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestDataPath.class);
    
    /**
     * Tests parsing a data path.
     * 
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    public void testParseSimple() throws ParseException
    {
        checkParse("/", new String[] {}, PathMode.ABSOLUTE);
        checkParse("", new String[] {}, PathMode.RELATIVE);
        checkParse("/abc", new String[] { "abc" }, PathMode.ABSOLUTE);
        checkParse("abc", new String[] { "abc" }, PathMode.RELATIVE);
    }
    
    /**
     * Tests parsing a path that has multiple attributes in a predicate.
     * 
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    public void testParsePredicate() throws ParseException
    {
        String s = "/applications/tuple[id1=123,id2=456]/degrees";
        DataPath d = new DataPath(s);
        
        // Check that the path is an absolute path
        assertEquals(PathMode.ABSOLUTE, d.getPathMode());
        
        // Check the first path step
        assertEquals(new DataPathStep("applications"), d.getPathSteps().get(0));
        
        // Check that the second path step has two attributes in the predicate
        Map<String, String> predicate = new LinkedHashMap<String, String>();
        predicate.put("id1", "123");
        predicate.put("id2", "456");
        assertEquals(new DataPathStep("tuple", predicate), d.getPathSteps().get(1));
        
        // Check the third path step
        assertEquals(new DataPathStep("degrees"), d.getPathSteps().get(2));
        
        // Check that the path can be converted back to string again
        assertEquals(s, d.toString());
    }
    
    /**
     * Tests finding a value in a data tree.
     * 
     * @throws ParseException
     *             if an error occurs during parsing.
     * @throws XmlParserException
     *             exception.
     */
    public void testFind() throws ParseException, XmlParserException
    {
        String source = "TestDataPath-testFind.xml";
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
        StringValue first_name = (StringValue) name.getAttribute("first_name");
        StringValue last_name = (StringValue) name.getAttribute("last_name");
        SwitchValue applying_for = (SwitchValue) application.getAttribute("applying_for");
        TupleValue phd = (TupleValue) applying_for.getCase();
        StringValue advisor_choice = (StringValue) phd.getAttribute("advisor_choice");
        CollectionValue degrees = (CollectionValue) application.getAttribute("degrees");
        TupleValue degree_1 = (TupleValue) degrees.getTuples().get(0);
        IntegerValue degree_1_id = (IntegerValue) degree_1.getAttribute("degree_id");
        StringValue degree_1_school = (StringValue) degree_1.getAttribute("school");
        IntegerValue degree_1_year = (IntegerValue) degree_1.getAttribute("year");
        TupleValue degree_3 = (TupleValue) degrees.getTuples().get(1);
        IntegerValue degree_3_id = (IntegerValue) degree_3.getAttribute("degree_id");
        StringValue degree_3_school = (StringValue) degree_3.getAttribute("school");
        NullValue degree_3_year = (NullValue) degree_3.getAttribute("year");
        
        // Check each value with its corresponding absolute path
        checkFindAbsolute(result, "/");
        checkFindAbsolute(user_id, "/user_id");
        checkFindAbsolute(applications, "/applications");
        checkFindAbsolute(application, "/applications/tuple[application_id=123]");
        checkFindAbsolute(application_id, "/applications/tuple[application_id=123]/application_id");
        checkFindAbsolute(name, "/applications/tuple[application_id=123]/name");
        checkFindAbsolute(first_name, "/applications/tuple[application_id=123]/name/first_name");
        checkFindAbsolute(last_name, "/applications/tuple[application_id=123]/name/last_name");
        checkFindAbsolute(applying_for, "/applications/tuple[application_id=123]/applying_for");
        checkFindAbsolute(phd, "/applications/tuple[application_id=123]/applying_for/phd");
        checkFindAbsolute(advisor_choice, "/applications/tuple[application_id=123]/applying_for/phd/advisor_choice");
        checkFindAbsolute(degrees, "/applications/tuple[application_id=123]/degrees");
        checkFindAbsolute(degree_1, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=1]");
        checkFindAbsolute(degree_1_id, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=1]/degree_id");
        checkFindAbsolute(degree_1_school, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=1]/school");
        checkFindAbsolute(degree_1_year, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=1]/year");
        checkFindAbsolute(degree_3, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=3]");
        checkFindAbsolute(degree_3_id, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=3]/degree_id");
        checkFindAbsolute(degree_3_school, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=3]/school");
        checkFindAbsolute(degree_3_year, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=3]/year");
        
        // Check each value with its corresponding relative path (from the "applications" collection value)
        checkFindRelative(applications, applications, "");
        checkFindRelative(applications, application, "tuple[application_id=123]");
        checkFindRelative(applications, application_id, "tuple[application_id=123]/application_id");
        checkFindRelative(applications, name, "tuple[application_id=123]/name");
        checkFindRelative(applications, first_name, "tuple[application_id=123]/name/first_name");
        checkFindRelative(applications, last_name, "tuple[application_id=123]/name/last_name");
        checkFindRelative(applications, applying_for, "tuple[application_id=123]/applying_for");
        checkFindRelative(applications, phd, "tuple[application_id=123]/applying_for/phd");
        checkFindRelative(applications, advisor_choice, "tuple[application_id=123]/applying_for/phd/advisor_choice");
        checkFindRelative(applications, degrees, "tuple[application_id=123]/degrees");
        checkFindRelative(applications, degree_1, "tuple[application_id=123]/degrees/tuple[degree_id=1]");
        checkFindRelative(applications, degree_1_id, "tuple[application_id=123]/degrees/tuple[degree_id=1]/degree_id");
        checkFindRelative(applications, degree_1_school, "tuple[application_id=123]/degrees/tuple[degree_id=1]/school");
        checkFindRelative(applications, degree_1_year, "tuple[application_id=123]/degrees/tuple[degree_id=1]/year");
        checkFindRelative(applications, degree_3, "tuple[application_id=123]/degrees/tuple[degree_id=3]");
        checkFindRelative(applications, degree_3_id, "tuple[application_id=123]/degrees/tuple[degree_id=3]/degree_id");
        checkFindRelative(applications, degree_3_school, "tuple[application_id=123]/degrees/tuple[degree_id=3]/school");
        checkFindRelative(applications, degree_3_year, "tuple[application_id=123]/degrees/tuple[degree_id=3]/year");
        
        checkFindNonExistent(data_tree, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=3]/school/non_existent");
        checkFindNonExistent(data_tree, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=3]/year/non_existent");
    }
    
    /**
     * Tests converting a data path to a schema path.
     * 
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    public void testToSchemaPath() throws ParseException
    {
        DataPath data_path = new DataPath("/applications/tuple[application_id=123]/degrees/tuple[degree_id=1]/degree_id");
        assertEquals(new SchemaPath("/applications/tuple/degrees/tuple/degree_id"), data_path.toSchemaPath());
    }
    
    public void testCreateDataPathFromTypeAndGlobalPrimaryKeys() throws ParseException, XmlParserException
    {
        String source = "TestDataPath-testFind.xml";
        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        SchemaTree source_schema = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        DataTree data_tree = ValueXmlParser.parseDataTree(XmlUtil.getOnlyChildElement(source_elem, "data_tree"), source_schema);
        
        checkCreateFromTypeAndGlobalKeys(data_tree, "/");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/user_id");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/application_id");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/name");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/name/first_name");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/name/last_name");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/applying_for");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/applying_for/phd");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/applying_for/phd/advisor_choice");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/degrees");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=1]");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=1]/degree_id");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=1]/school");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=1]/year");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=3]");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=3]/degree_id");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=3]/school");
        checkCreateFromTypeAndGlobalKeys(data_tree, "/applications/tuple[application_id=123]/degrees/tuple[degree_id=3]/year");
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
        
        DataPath path = new DataPath(path_string);
        
        List<DataPathStep> s = new ArrayList<DataPathStep>();
        for (String path_step : path_steps)
        {
            s.add(new DataPathStep(path_step));
        }
        
        assertEquals(s, path.getPathSteps());
        assertEquals(path_mode, path.getPathMode());
        assertEquals(path_string, path.toString());
    }
    
    /**
     * Checks that an absolute path can be used to find a value in the data tree.
     * 
     * @param value
     *            the value.
     * @param path_string
     *            the path in string format.
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    private static void checkFindAbsolute(Value value, String path_string) throws ParseException
    {
        assert (value != null);
        assert (path_string != null);
        assert (path_string.startsWith("/"));
        
        DataTree data_tree = (DataTree) value.getRoot();
        
        // Check that constructing a path from a string is equivalent to constructing one for a value node
        assertEquals(new DataPath(path_string), new DataPath(value));
        
        // Check that the path can be used to find the value again in the data tree
        assertEquals(value, new DataPath(path_string).find(data_tree));
        assertEquals(value, new DataPath(path_string).find(data_tree.getRootValue()));
    }
    
    /**
     * Checks that a relative path can be used to find a value in the data tree.
     * 
     * @param start_value
     *            the value to start finding from.
     * @param value
     *            the value.
     * @param path_string
     *            the path in string format.
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    private static void checkFindRelative(Value start_value, Value value, String path_string) throws ParseException
    {
        assert (value != null);
        assert (path_string != null);
        assert (!path_string.startsWith("/"));
        
        // Check that constructing a path from a string is equivalent to constructing one for a value node
        assertEquals(new DataPath(path_string), new DataPath(start_value, value));
        
        // Check that the path can be used to find the value again in the data tree
        assertEquals(value, new DataPath(path_string).find(start_value));
    }
    
    /**
     * Checks that a path to a non-existent value can be used on a data tree.
     * 
     * @param data_tree
     *            the data tree.
     * @param path_string
     *            the path in string format.
     * @throws ParseException
     *             if an error occurs during parsing.
     */
    private static void checkFindNonExistent(DataTree data_tree, String path_string) throws ParseException
    {
        assert (path_string != null);
        
        assertNull(new DataPath(path_string).find(data_tree));
        assertNull(new DataPath(path_string).find(data_tree.getRootValue()));
    }
    
    private static void checkCreateFromTypeAndGlobalKeys(DataTree data_tree, String path_string) throws ParseException
    {
        assert (data_tree != null);
        assert (path_string != null);
        assert (path_string.startsWith("/"));
        
        DataPath path_from_string = new DataPath(path_string);
        Value value = path_from_string.find(data_tree);
        assert (value != null);
        List<String> keys = new ArrayList<String>();
        TupleValue tuple = value.getLowestCollectionTupleAncestor();
        if (tuple != null)
        {
            for (ScalarValue key : PrimaryKeyUtil.getGlobalPrimaryKeyValues(tuple))
            {
                keys.add(key.toString());
            }
        }
        DataPath path_from_type_and_keys = new DataPath(value.getType(), keys);
        assertEquals(path_from_string, path_from_type_and_keys);
    }
}
