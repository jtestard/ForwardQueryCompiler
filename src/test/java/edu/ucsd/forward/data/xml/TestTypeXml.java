/**
 * 
 */
package edu.ucsd.forward.data.xml;

import static edu.ucsd.forward.xml.XmlUtil.createDocument;

import java.util.Collections;

import org.testng.annotations.Test;

import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceXmlParser;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.test.AbstractTestCase;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Unit tests for the type XML parser and serializer.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 */
@Test
public class TestTypeXml extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestTypeXml.class);
    
    /**
     * Tests parsing and serializing a schema tree with constraints.
     * 
     * @throws Exception
     *             exception.
     */
    public void testTypeXml() throws Exception
    {
        String source = "TestTypeXml-testTypeXml.xml";
        Element root_elm = (Element) XmlUtil.parseDomNode(IoUtil.getResourceAsString(this.getClass(), source));
        
        // Build input data sources and schema and data objects
        UnifiedApplicationState uas = new UnifiedApplicationState(Collections.<DataSource> emptyList());
        for (DataSource data_source : DataSourceXmlParser.parse(root_elm, new LocationImpl(source)))
        {
            uas.addDataSource(data_source);
        }
        
        for (Element data_obj_elm : XmlUtil.getChildElements(root_elm))
        {
            String schema_obj_name = XmlUtil.getAttribute(data_obj_elm, DataSourceXmlParser.NAME_ATTR);
            SchemaTree schema_tree = uas.getDataSource(DataSource.MEDIATOR).getSchemaObject(schema_obj_name).getSchemaTree();
            
            Document result_doc = createDocument();
            TypeXmlSerializer.serializeSchemaTree(schema_tree, result_doc);
            
            Element schema_elm = XmlUtil.getOnlyChildElement(data_obj_elm, "schema_tree");
            
            assertEquals(XmlUtil.serializeDomToString(schema_elm, true, true), XmlUtil.serializeDomToString(result_doc, true, true));
        }
    }
}
