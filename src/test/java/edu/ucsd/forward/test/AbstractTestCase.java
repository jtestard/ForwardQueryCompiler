/**
 * 
 */
package edu.ucsd.forward.test;

import static edu.ucsd.forward.xml.XmlUtil.createDocument;
import static edu.ucsd.forward.xml.XmlUtil.serializeDomToString;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.testng.AssertJUnit;

import com.google.gwt.xml.client.Document;

import edu.ucsd.app2you.util.IoUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.xml.ValueXmlSerializer;

/**
 * Abstract class for implementing test classes.
 * 
 * @author Kevin Zhao
 * @author Kian Win
 * 
 */
public abstract class AbstractTestCase extends AssertJUnit
{
    private static final Logger log                     = Logger.getLogger(AbstractTestCase.class);
    
    public static final String  FAILURE                 = "failure";
    
    public static final String  PERFORMANCE             = "performance";
    
    public static final String  NORMAL_FORM_REFACTORING = "normal_form_refactoring";
    
    // FORWARD-specific test cases
    
    /**
     * Asserts that two types are deep equal by isomorphism. If they are not, an AssertionError is thrown.
     * 
     * @param <T>
     *            the type
     * @param actual
     *            the actual value
     * @param expected
     *            the expected value
     */
    public static <T extends Type> void assertDeepEqualByIsomorphism(T expected, T actual)
    {
        assertDeepEqualByIsomorphism(null, actual, expected, false);
    }
    
    /**
     * Asserts that two types are deep equal by isomorphism including constraints. If they are not, an AssertionError is thrown.
     * 
     * @param <T>
     *            the type
     * @param actual
     *            the actual value
     * @param expected
     *            the expected value
     */
    public static <T extends Type> void assertDeepEqualByIsomorphismWithConstraints(T expected, T actual)
    {
        assertDeepEqualByIsomorphism(null, actual, expected, true);
    }
    
    /**
     * Asserts that two types are deep equal by isomorphism. If they are not, an AssertionError, with the given message, is thrown.
     * 
     * @param <T>
     *            the type
     * @param actual
     *            the actual value
     * @param expected
     *            the expected value
     * @param message
     *            the assertion error message
     * @param with_constraints
     *            whether the comparison should include constraints
     */
    public static <T extends Type> void assertDeepEqualByIsomorphism(String message, T expected, T actual, boolean with_constraints)
    {
        if (!TypeUtil.deepEqualsByIsomorphism(expected, actual, with_constraints))
        {
            fail(format(message, actual, expected));
        }
    }
    
    /**
     * Asserts that two values are equal by xml serialization. If they are not, an AssertionError is thrown.
     * 
     * Consider using assertDeepEqualWithTypeInfo.
     * 
     * @param actual
     *            the actual value
     * @param expected
     *            the expected value
     */
    public static void assertEqualSerialization(Value expected, Value actual)
    {
        assert (!(expected instanceof DataTree) && !(actual instanceof DataTree)) : "Right now we cannot handle data tree here";
        assertEqualSerialization(null, expected, actual);
    }
    
    /**
     * Asserts that two values are equal by xml serialization. If they are not, an AssertionError, with the given message, is
     * thrown.
     * 
     * @param actual
     *            the actual value
     * @param expected
     *            the expected value
     * @param message
     *            the assertion error message
     */
    public static void assertEqualSerialization(String message, Value expected, Value actual)
    {
        Document actual_doc = createDocument();
        ValueXmlSerializer.serializeValue(actual, "root", actual_doc);
        
        Document expected_doc = createDocument();
        ValueXmlSerializer.serializeValue(expected, "root", expected_doc);
        
        // Check that the XML trees are isomorphic
        String expected_doc_str = serializeDomToString(expected_doc, true, true);
        String actual_doc_str = serializeDomToString(actual_doc, true, true);
        if (!actual_doc_str.equals(expected_doc_str))
        {
            fail(format(message, expected, actual));
        }
    }
    
    /**
     * Asserts that two values are deep equal by isomorphism. If they are not, an AssertionError, with the given message, is thrown.
     * This method requires the values are mapped with types. And collection types have local primary key constraints defined.
     * 
     * @param actual
     *            the actual value
     * @param expected
     *            the expected value
     * @param message
     *            the assertion error message
     */
    public static void assertDeepEqualWithTypeInfo(String message, Value expected, Value actual)
    {
        // Check that the XML trees are isomorphic
        if (!ValueUtil.deepEquals(expected, actual))
        {
            fail(format(message, expected, actual));
        }
    }
    
    /**
     * Asserts that two given strings equal after IoUtil.normalizeLineBreak.
     * 
     * @param expected
     *            the expected string
     * @param actual
     *            the actual string
     */
    public static void assertEqualsWithNormalizeLineBreak(String expected, String actual)
    {
        assertEquals(IoUtil.normalizeLineBreak(expected), IoUtil.normalizeLineBreak(actual));
    }
    
    /**
     * Copied from org.testng.Assert since it was not visible.
     * 
     * @param message
     *            optional additional message
     * @param expected
     *            expected object
     * @param actual
     *            actual object
     * @return a formated string
     */
    static String format(String message, Object expected, Object actual)
    {
        String formatted = "";
        if (message != null)
        {
            formatted = message + " ";
        }
        
        return formatted + "expected:<" + expected + "> but was:<" + actual + ">";
    }
    
    public void checkSerialization(String actual, String file_name, Class<?> resource_class)
    {
        String expected = IoUtil.getResourceAsString(resource_class, file_name);
        assertEquals(IoUtil.normalizeLineBreak(expected.trim()), IoUtil.normalizeLineBreak(actual.trim()));
    }
    
    /**
     * Update the resource SOURCE file (rather than target file) with the text content from the actual string. Kevin: I am doing
     * string replacement to get the source file URL. Using this method at your own risk!
     * 
     * @param actual
     *            the actual text content used to replace the file content.
     * @param file_name
     *            the file name
     * @param resource_class
     *            the resource class
     */
    public void updateSerialization(String actual, String file_name, Class<?> resource_class)
    {
        URL url = resource_class.getResource(file_name);
        try
        {
            String src_url = url.toString().replace("/target/test-classes/", "/src/test/resources/");
            File file = new File(new URI(src_url));
            FileWriter writer = new FileWriter(file);
            
            log.info("Writing file: {}", url);
            writer.write(actual);
            writer.close();
        }
        catch (URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        
    }
}
