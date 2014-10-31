/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.xml.TypeXmlParser;
import edu.ucsd.forward.data.xml.ValueXmlParser;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Unit tests for order constraint.
 * 
 * @author Yupeng Fu
 * 
 */
@Test
public class TestOrderConstraint extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestOrderConstraint.class);
    
    public void testOrderConstraintChecker() throws Exception
    {
        String source = "TestOrderConstraint-testOrderConstraintChecker.xml";
        Element source_elem = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        SchemaTree schema_tree = TypeXmlParser.parseSchemaTree(XmlUtil.getOnlyChildElement(source_elem, "schema_tree"));
        DataTree data_tree = ValueXmlParser.parseDataTree(XmlUtil.getOnlyChildElement(source_elem, "data_tree"), schema_tree);
        
        OrderConstraint constraint = (OrderConstraint)schema_tree.getRootType().getConstraints().get(1);
        CollectionValue collection = (CollectionValue) data_tree.getRootValue();
        ConstraintChecker checker= constraint.newConstraintChecker(collection);
        assert checker.check();
        
        // Add a tuple that is out of order
        TupleValue tuple = new TupleValue();
        tuple.setAttribute("application_id", new IntegerValue(4));
        tuple.setAttribute("first_name", new StringValue("Alice"));
        tuple.setAttribute("last_name", new StringValue("James"));
        collection.add(tuple);
        
        assert !checker.check();
    }
}
