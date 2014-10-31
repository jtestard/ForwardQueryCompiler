/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import java.util.List;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.exception.ExceptionMessages.ActionCompilation;
import edu.ucsd.forward.exception.ExceptionMessages.Function;
import edu.ucsd.forward.fpl.AbstractFplTestCase;
import edu.ucsd.forward.fpl.FplCompilationException;
import edu.ucsd.forward.fpl.FplInterpreter;
import edu.ucsd.forward.fpl.FplInterpreterFactory;
import edu.ucsd.forward.fpl.ast.ActionDefinition;
import edu.ucsd.forward.fpl.ast.Definition;
import edu.ucsd.forward.fpl.ast.FunctionDefinition;
import edu.ucsd.forward.query.function.FunctionRegistryException;

/**
 * Tests action compilation errors.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestFplCompilationExceptions extends AbstractFplTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestFplCompilationExceptions.class);
    
    private static final String NL  = "\n";
    
    /**
     * Tests an action compilation exception message.
     */
    public void test()
    {
        String expected = "";
        String actual = "";
        CheckedException ex = null;
        CheckedException cause = null;
        
        Location location = new LocationImpl("TestFplCompilationExceptions-test.xml");
        
        try
        {
            parseTestCase(TestFplCompilationExceptions.class, location.getPath());
        }
        catch (CheckedException e)
        {
            assert (false);
        }
        
        FplInterpreter interpreter = FplInterpreterFactory.getInstance();
        interpreter.setUnifiedApplicationState(getUnifiedApplicationState());
        
        for (String expr : getActionExpressions())
        {
            try
            {
                List<Definition> definitions = interpreter.parseFplCode(expr, location);
                for (Definition definition : definitions)
                {
                    if (definition instanceof ActionDefinition) continue;
                    interpreter.registerFplFunction((FunctionDefinition) definition);
                }
                for (Definition definition : definitions)
                {
                    if (definition instanceof ActionDefinition) continue;
                    interpreter.compileFplFunction((FunctionDefinition) definition);
                }
            }
            catch (CheckedException e)
            {
                actual += e.getSingleLineMessageWithLocation() + NL;
            }
        }
        
        // DUPLICATE EXCEPTION DECLARATION.
        ex = new FplCompilationException(ActionCompilation.DUPLICATE_EXCEPTION,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 6, 5), "expected_exception");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // UNDECLARED_EXCEPTION
        ex = new FplCompilationException(ActionCompilation.UNDECLARED_EXCEPTION,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 5, 11), "undeclared_exception");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // UNHANDLED EXCEPTION
        ex = new FplCompilationException(ActionCompilation.UNHANDLED_EXCEPTION,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 5, 5), "unhandled");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // DUPLICATED FUNCTION NAME
        cause = new FunctionRegistryException(Function.DUPLICATE_FUNCTION, "duplicate_function");
        ex = new FplCompilationException(ActionCompilation.FUNCTION_REGISTRATION_EXCEPTION,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 7, 15), cause);
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // DUPLICATED PARAMETER NAME
        ex = new FplCompilationException(ActionCompilation.DUPLICATE_PARAMETER_NAME,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 3, 51), "p");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // DUPLICATED VARIABLE NAME
        ex = new FplCompilationException(ActionCompilation.DUPLICATE_VARIABLE_NAME,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 6, 5), "v");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // INVALID RETURN TYPE
        ex = new FplCompilationException(ActionCompilation.INVALID_RETURN_TYPE,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 5, 5), "void", "integer");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        ex = new FplCompilationException(ActionCompilation.INVALID_RETURN_TYPE,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 5, 5), "integer", "void");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        ex = new FplCompilationException(ActionCompilation.INVALID_RETURN_TYPE,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 5, 5), "integer", "string");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // INVALID IF BRANCH TYPE
        ex = new FplCompilationException(ActionCompilation.INVALID_IF_CONDITION_TYPE,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 5, 8), "'string'");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // UNREACHABLE STATEMENT
        ex = new FplCompilationException(ActionCompilation.UNREACHABLE_STATEMENT,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 6, 5), "RETURN 1");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        ex = new FplCompilationException(ActionCompilation.UNREACHABLE_STATEMENT,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 8, 5), "RETURN 1");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        ex = new FplCompilationException(ActionCompilation.UNREACHABLE_STATEMENT,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 7, 5), "RETURN 0");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        ex = new FplCompilationException(ActionCompilation.UNREACHABLE_STATEMENT,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 14, 5), "RETURN 3");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        // INVALID DEFAULT VALUE TYPE
        ex = new FplCompilationException(ActionCompilation.INVALID_VARIABLE_DEFAULT_VALUE_TYPE,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 5, 5), "integer", "boolean");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        // ASSIGNMENT TYPE ERROR
        ex = new FplCompilationException(ActionCompilation.UNMATCH_ASSIGNMENT_TYPE,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 7, 5, 7, 5), "integer",
                                         "boolean");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // NOT RETURNED FUNCTION
        ex = new FplCompilationException(ActionCompilation.MISSING_RETURN_STATEMENT,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 3, 15), "integer", "boolean");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        // NOT RETURNED HANDLER
        ex = new FplCompilationException(ActionCompilation.MISSING_RETURN_STATEMENT_IN_HANDLER,
                                         new LocationImpl("TestFplCompilationExceptions-test.xml", 9, 1), "integer", "boolean");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        assertEquals(expected, actual);
    }
}
