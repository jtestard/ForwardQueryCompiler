/**
 * 
 */
package edu.ucsd.forward.query.logical;

import java.util.Collections;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.CheckedException;
import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.LocationImpl;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.exception.ExceptionMessages.QueryParsing;
import edu.ucsd.forward.query.AbstractQueryTestCase;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryParsingException;
import edu.ucsd.forward.query.QueryProcessor;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.function.FunctionRegistryException;

/**
 * Tests query compilation errors.
 * 
 * @author Michalis Petropoulos
 * 
 */
@Test
public class TestQueryCompilationExceptions extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestQueryCompilationExceptions.class);
    
    private static final String NL  = "\n";
    
    /**
     * Tests a query compilation exception message.
     */
    public void test()
    {
        String expected = "";
        String actual = "";
        CheckedException ex = null;
        CheckedException cause = null;
        
        Location location = new LocationImpl("TestQueryCompilationExceptions-test.xml");
        
        QueryProcessor processor = QueryProcessorFactory.getInstance();
        try
        {
            parseTestCase(TestQueryCompilationExceptions.class, location.getPath());
        }
        catch (CheckedException e)
        {
            throw new AssertionError(e);
        }
        
        for (String expr : getQueryExpressions())
        {
            try
            {
                AstTree ast_tree = processor.parseQuery(expr, location).get(0);
                processor.compile(Collections.singletonList(ast_tree), getUnifiedApplicationState());
            }
            catch (CheckedException e)
            {
                actual += e.getSingleLineMessageWithLocation() + NL;
            }
        }
        
        // QUERY_PARSE_ERROR
        ex = new QueryParsingException(QueryParsing.QUERY_PARSE_ERROR, new LocationImpl(location.getPath(), 3, 13, 3, 17),
                                       "Encountered \"WHERE\"");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // CONSECUTIVE_OFFSET_CLAUSES
        ex = new QueryParsingException(QueryParsing.CONSECUTIVE_OFFSET_CLAUSES, new LocationImpl(location.getPath(), 3, 62));
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // CONSECUTIVE_FETCH_CLAUSES
        ex = new QueryParsingException(QueryParsing.CONSECUTIVE_FETCH_CLAUSES, new LocationImpl(location.getPath(), 3, 86));
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // CONSECUTIVE_ORDER_BY_CLAUSES
        // FIXME Add location information
        ex = new QueryParsingException(QueryParsing.CONSECUTIVE_ORDER_BY_CLAUSES,
                                       new LocationImpl(location.getPath(), 3, 69, 3, 70));
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // INVALID_QUERY_PATH
        ex = new QueryCompilationException(QueryCompilation.UNKNOWN_ATTRIBUTE,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 13, 3, 28),
                                           "mem_db.no_object");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // UNKNOWN_PREFIX
        ex = new QueryCompilationException(QueryCompilation.UNKNOWN_ATTRIBUTE,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 13, 3, 31),
                                           "no_source.no_object");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        ex = new QueryCompilationException(QueryCompilation.UNKNOWN_ATTRIBUTE,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 20, 3, 28), "no_step");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // UNKNOWN_ATTRIBUTE
        ex = new QueryCompilationException(QueryCompilation.UNKNOWN_ATTRIBUTE,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 13, 3, 36), "no_step");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        ex = new QueryCompilationException(QueryCompilation.UNKNOWN_ATTRIBUTE,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 20, 3, 26), "no_step");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // CROSSING_COLLECTION
        ex = new QueryCompilationException(QueryCompilation.STARTING_WITH_COLLECTION,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 13, 3, 31));
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        ex = new QueryCompilationException(QueryCompilation.CROSSING_COLLECTION,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 13, 3, 37), "phones",
                                           "mem_db.user.phones.number");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // INVALID_FUNCTION_CALL
        cause = new FunctionRegistryException(ExceptionMessages.Function.UNKNOWN_FUNCTION_NAME, "no_function");
        ex = new QueryCompilationException(QueryCompilation.INVALID_FUNCTION_CALL,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 13), cause);
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // NO_FUNCTION_SIGNATURE
        ex = new QueryCompilationException(QueryCompilation.NO_FUNCTION_SIGNATURE,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 13),
                                           "MOD('no_signature', 'no_signature')");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        ex = new QueryCompilationException(QueryCompilation.NO_FUNCTION_SIGNATURE,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 20), "AVG");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // Kevin: I disabled this because we allow tuple with multiple attributes to be casted to a scalar, for key inference.
        // // INVALID_TYPE_CONVERION
        // ex = new QueryCompilationException(QueryCompilation.INVALID_TYPE_CONVERION,
        // new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 13), "tuple", "integer");
        // expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // NON_BOOL_CASE_BRANCH_CONDITION
        ex = new QueryCompilationException(QueryCompilation.INVALID_CASE_CONDITION_TYPE,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 13), "1");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // INCONSISTENT_CASE_BRANCH_TYPE
        ex = new QueryCompilationException(QueryCompilation.INCONSISTENT_CASE_BRANCH_TYPE,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 13), "2");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // Erick: Disabled this because we allow non-scalar types to be returned from a case branch
        // NON_SCALAR_CASE_BRANCH_TYPE
        // ex = new QueryCompilationException(QueryCompilation.NON_SCALAR_CASE_BRANCH_TYPE,
        // new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 13), "1");
        // expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // DUPLICATE_SELECT_ITEM_ALIAS
        ex = new QueryCompilationException(QueryCompilation.DUPLICATE_SELECT_ITEM_ALIAS,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 28), "a");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // DUPLICATE_FROM_ITEM_ALIAS
        ex = new QueryCompilationException(QueryCompilation.DUPLICATE_FROM_ITEM_ALIAS,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 50, 3, 70), "a");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // INVALID_OFFSET_PROVIDED_TYPE
        ex = new QueryCompilationException(QueryCompilation.INVALID_OFFSET_PROVIDED_TYPE,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 44));
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // INVALID_FETCH_PROVIDED_TYPE
        ex = new QueryCompilationException(QueryCompilation.INVALID_FETCH_PROVIDED_TYPE,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 75));
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // DML_INVALID_PROVIDED_TYPE
        ex = new QueryCompilationException(QueryCompilation.DML_INVALID_PROVIDED_TYPE,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 4, 17, 4, 18));
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // DML_NON_COLLECTION_TARGET_TYPE
        ex = new QueryCompilationException(QueryCompilation.DML_NON_COLLECTION_TARGET_TYPE,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 25, 3, 35));
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // DML_UNEQUAL_PROVIDED_TARGET_ATTRS
        ex = new QueryCompilationException(QueryCompilation.DML_UNEQUAL_PROVIDED_TARGET_ATTRS,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 25, 3, 40),
                                           "mem_db.proposals");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        // DML_INVALID_TARGET_ATTR
        ex = new QueryCompilationException(QueryCompilation.DML_INVALID_TARGET_ATTR,
                                           new LocationImpl("TestQueryCompilationExceptions-test.xml", 3, 25, 3, 40), "no_attr",
                                           "mem_db.proposals");
        expected += ex.getSingleLineMessageWithLocation() + NL;
        
        assertEquals(expected, actual);
    }
}
