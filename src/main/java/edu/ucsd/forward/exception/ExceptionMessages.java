package edu.ucsd.forward.exception;

import edu.ucsd.forward.data.source.DataSourceMetaData.DataModel;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;

/**
 * A list of error messages for CheckedUserExceptions.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public final class ExceptionMessages
{
    /**
     * Hidden constructor.
     */
    private ExceptionMessages()
    {
    }
    
    /**
     * A list of error messages related to types.
     */
    public enum Type implements Messages
    {
        // General exceptions
        UNKNOWN_TYPE_NAME("Type name \"{0}\" cannot be resolved"),
        
        // Conversion exceptions
        INVALID_TYPE_CONVERION("Cannot convert type \"{0}\" to type \"{1}\""),
        
        INVALID_COLLECTION_TO_TUPLE_CONVERION("Cannot convert a collection value with mutiple tuples to a tuple value"),
        
        INVALID_COLLECTION_TO_SCALAR_CONVERION(
                "Cannot convert a collection value with mutiple tuples or multiple attributes to a scalar value"),
        
        @Deprecated
        INVALID_TUPLE_TO_SCALAR_CONVERION("Cannot convert a tuple value with mutiple attributes to a scalar value"),
        
        INVALID_STRING_TO_TYPE_CONVERION("Cannot convert string value '{0}' to \"{1}\" value"),
        
        OUT_OF_RANGLE_DOWNCAST_CONVERION("Cannot convert value \"{0}\" to type \"{1}\" because value is out of range"),
        
        // Schema exceptions
        INVALID_SCHEMA_PATH("Invalid path \"{0}\""),
        
        INVALID_SCHEMA_PATH_SCALAR("Path \"{0}\" does not lead to a scalar type"),
        
        // Java to SQL++
        INVALID_JAVA_TYPE_MAPPING("Cannot map a Java object of \"{0}\" class to SQL++ value of \"{1}\" type"),
        
        MORE_THAN_ONE_SWITCH_CASE("The Java object of \"{0}\" class maps more than switch cases"),
        
        NON_EXISTING_GETTER(
                "Cannot navigate with attribute \"{0}\" into Java object of \"{1}\" class, because the class does not have getter \"{0}\""),
        
        ERROR_INVOKING_GETTER("Cannot invoke the getter \"{0}\" in Java object of \"{1}\" class."),
        
        // JSON to SQL++
        INVALID_JSON_TYPE_MAPPING("Cannot map a JSON element \"{0}\" to SQL++ scalar value"),
        
        ERROR_GETTING_JSON("No corresponding member for \"{0}\" in given JSON object"),
        
        ERROR_ITERATING_JSON("The JSON object \"{0}\" is not an array to iterate");
        
        private static final int  MODULE = 11;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        Type(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        Type(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages related to functions.
     */
    public enum Function implements Messages
    {
        // General exceptions
        UNKNOWN_FUNCTION_NAME("Function name \"{0}\" cannot be resolved"),
        
        DUPLICATE_FUNCTION("The function \"{0}\" already exists"),
        
        NON_EXISTING_FUNCTION("The function \"{0}\" does not exist"),
        
        FUNCTION_INSTANTIATION_ERROR("Unable to instantiate function \"{0}\"");
        
        private static final int  MODULE = 21;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        Function(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        Function(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for data sources and unified application state.
     */
    public enum DataSource implements Messages
    {
        // Data source configuration exceptions
        CONFIG_ERROR("Error configuring data source \"{0}\""),
        
        VIEW_ERROR("Error configuring view \"{0}\""),
        
        MISSING_ATTR("Attribute \"{0}\" is missing"),
        
        SAX_EXCEPTION("Error parsing XML file"),
        
        INVALID_NEW_DATA_SRC_NAME("Data source name \"{0}\" is already in use"),
        
        UNKNOWN_DATA_MODEL("Data model \"{0}\" is not supported"),
        
        UNKNOWN_STORAGE_SYSTEM("Storage system \"{0}\" is not supported"),
        
        UNKNOWN_SITE("The specified site \"{0}\" should be either CLIENT or SERVER"),
        
        UNKNOWN_CARDINALITY_ESTIMATE("Cardinality estimate \"{0}\" is not supported"),
        
        NO_JDBC_DRIVER("No connection driver is provided for JDBC data source \"{0}\""),
        
        NO_JDBC_PROPERTIES("User/Password properties (one or both) are not provided for JDBC data source \"{0}\""),
        
        INVALID_JDBC_MODEL("The data model of JDBC data source \"{0}\" must be " + DataModel.RELATIONAL.name()),
        
        INVALID_JDBC_ENVIRONMENT("JDBC data source \"{0}\" has more than one set of properties for environment \"{1}\""),
        
        MISSING_JDBC_ENVIRONMENT("JDBC data source \"{0}\" has no properties for environment \"{1}\""),
        
        JDBC_POOL_ERROR("Error initializing the connection pool for JDBC data source \"{0}\""),
        
        INVALID_JDBC_STORAGE_SYSTEM("The storage system of data source \"{0}\" should be " + StorageSystem.JDBC.name()),
        
        INVALID_STORAGE_SYSTEM("The storage system of data source \"{0}\" should not be \"{1}\""),
        
        REMOTE_CONNECTIONS_NOT_ALLOWED("Only database connections to \"localhost\" are allowed."),
        
        // AsterixDB related exceptions
        
        INVALID_ASTERIX_MODEL("The data model of ASTERIX data source \"{0}\" must be " + DataModel.ADM.name()),
        
        MISSING_ASTERIX_ENVIRONMENT("ASTERIX data source \"{0}\" has no properties for environment \"{1}\""),
        
        // IndexedDB related exceptions
        INVALID_IDB_MODEL("The data model of IndexedDB data source \"{0}\" must be " + DataModel.RELATIONAL.name()),
        
        IDB_MISSING_PK("The data object \"{0}\" in IndexedDB data source \"{1}\" must have primary key "),
        
        // Data source import exceptions
        UNKNOWN_JDBC_SCHEMA("Schema \"{0}\" in data source \"{1}\" does not exist"),
        
        JDBC_SCHEMA_METADATA("Error reading metadata for schema \"{0}\" in JDBC data source \"{1}\""),
        
        JDBC_TABLE_METADATA("Error reading metadata for table \"{0}\" in JDBC data source \"{1}\""),
        
        INVALID_OPEN_STATE("Cannot add an open data source \"{0}\" to a closed UAS"),
        
        // Data source access exceptions
        INVALID_NEW_SCHEMA_OBJ_NAME("Data object name \"{0}\" in data source \"{1}\" is already in use"),
        
        UNKNOWN_DATA_SRC_NAME("Data source \"{0}\" does not exist"),
        
        UNKNOWN_SCHEMA_OBJ_NAME("Data object \"{0}\" does not exist in data source \"{1}\""),
        
        UNKNOWN_DATA_OBJ_NAME("Data object \"{0}\" does not exist in data source \"{1}\""),
        
        // Index related exceptions
        INVALID_NEW_INDEX_NAME("Index name \"{0}\" in collection \"{1}\" is already in use."),
        
        UNKNOWN_INDEX_NAME("Collection \"{0}\" does not have index named \"{1}\""),
        
        UNKNOWN_COLLECTION_PATH("The collection \"{0}\" to create index does not exist."),
        
        UNKNOWN_KEY_PATH("The index key path \"{0}\" does not exist."),
        
        NON_EXISTING_INDEX("The index \"{0}\" in collection \"{1}\" does not exist "),
        
        // FPL local storage exceptions
        UNCLEANED_UP_FPL_STACK("The FPL local storage stack still has leftover"),
        
        EMPTY_FPL_STACK("The FPL local storage stack is empty."),
        
        // Asynchronous access required exception
        
        ASYNC_ACCESS_REQUIRED("Data object \"{0}\" needs asynchronous access in data source \"{1}\""),
        
        // Unsupported posgresql
        
        UNSUPPORTED_POSTGRESQL("Unsupported posgresql version. Minimum required version supported is 8.1");
        
        private static final int  MODULE = 31;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        DataSource(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        DataSource(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
        
    }
    
    /**
     * A list of error messages for the old query compilation.
     */
    public enum OldQueryCompilation implements Messages
    {
        // General exceptions
        
        // Query reference checker exceptions
        INVALID_KEY_ON_EXPRESSION("Invalid KEY ON expression {0}, only reference to the output attribute is allowed"), NON_EXIST_ATTRIBUTE(
                "Attribute {0} referenced by {1} does not exist"), NON_EXIST_DIRECT_ATTRIBUTE("Attribute {0} does not exist"), MISSING_CASE_NAME(
                "The case name of the expression {0} is missing in the switch constructor {1}"), MISSING_TUPLE_ATTRIBUTE_NAME(
                "The name of the tuple attribute {0} is missing in the tuple constructor {1}"), MISSING_FROM_ITEM_ALIAS(
                "The alias is missing for the FROM item {0}  "), MISSING_SELECT_ITEM_ALIAS(
                "The alias is missing for the SELECT item {0}"), DUPLICATE_CASE_NAME(
                "The case name {0} is used more than once in the switch constructor {1}"), DUPLICATE_TUPLE_ATTRIBUTE_NAME(
                "The attribute name {0} is used more than once in the tuple constructor {1}"), DUPLICATE_FROM_ITEM_ALIAS(
                "The alias {0} for FROM item {1} is already used by previous FROM item"), DUPLICATE_SELECT_ITEM_ALIAS(
                "The alias {0} for SELECT item {1} is already used by previous SELECT item"), AMBIGUOUS_ATTRIBUTE_REFERENCE(
                "The attribute reference {0} is ambiguous"), REFERENCE_OUT_OF_SCOPE("The attribute reference {0} is out of scope"), NON_EXIST_FUNCTION(
                "Function {0} does not exist"), NON_EXIST_VARIABLE("The variable {0} does not exist, should be {1}"),
        
        // Query type checker exceptions
        INVALID_ARG_TYPE("Argument of {0} must be type {1}, not type {2}  "), NOT_MATCH_ARG_NUMBER(
                "Function {0} takes {1} argument(s), not {2}"), INVALID_ARGUMENT_IN_SCALAR_ELEMENT_FUNCTION(
                "The query in SCALAR_ELEMENT function {0} should have only one SELECT item"), NO_MATCH_FUNCTION(
                "No function matches the given name and argument(s): {0} "), INCONSISTENT_CASE_BRANCH_TYPE(
                "All the type of the CASE branches {0} must be the same, the type of branch {1} is {2}, but previous branch is {3}"), INVALD_FROM_ITEM_TYPE(
                "Invalid type of the FROM ITEM {0}"), INVALID_CONDITION_TYPE("Condition {0} must be type boolean, not type {1}"), NON_MATCH_TYPE_IN_OUTER_UNION(
                "Non-matching types {0} and {1} navigated by {2} are found in the OUTER UNION query {3}"), NON_SCALAR_ORDER_BY_TYPE(
                "The ORDER BY item {0} must be scalar type, not type {1}"), NON_INTERGER_LIMIT_EXPR(
                "The LIMIT expression {0} must be integer type, not type {1}"), NON_INTERGER_OFFSET_EXPR(
                "The OFFSET expression {0} must be integer type, not type {1}"), NON_SCALAR_KEY_ON_TYPE(
                "The KEY ON item {0} must be scalar type, not type {1}"), DIFFERENT_UNION_TYPE(
                "Each UNION query must have the same collection type in {0} "), NON_SCALAR_GROUP_BY_TYPE(
                "The GROUP BY item {0} must be scalar type, not type {1}"), NON_COMPARABLE_TYPES(
                "The left hand side type {0} and right hand side type {1} are not comparable in {2} "), COLLECTION_TYPE_IN_NULL_FUNCTION(
                "Collection type expression {0} cannot be used in IS NULL function"), INVALID_SELECT_ITEM_TYPE_IN_SCALAR_ELEMENT_QUERY(
                "The SELECT ITEM of the query {0} in SCALAR_ELEMENT function should be type scalar, not type {1}"), INVALID_MATH_FUNCTION_TYPE(
                "Math function {0} must take numeric type argument"), FORWARD_FEATURE_IN_SQL_FUNCTION(
                "The FORWARD construct {0} could not be used in SQL function {1}"),
        
        // Query key checker exceptions
        INCONSISTENT_UNION_QUERY_PRIMARY_KEY(
                "The primary key(s) {0} of the UNION query {1} is different from the primary keys {2} of another branch"), NON_SCALAR_TYPE_IN_QUERY_SELECT_ITEM(
                "There must be at least one scalar type SELECT item in query {0}"), GROUP_BY_ITEM_NOT_DECLARED_IN_SELECT_CLAUSE(
                "The GROUP BY item {0} is not declared in the SELECT clause"), FROM_ITEM_KEY_NOT_DECLARED_IN_SELECT_CLAUSE(
                "The primary key {1} of FROM item's {0} is not declared in the SELECT clause"), PRIMARY_KEY_NOT_SPECIFIED(
                "The primary key {0} is not provided"),
        
        // Query group by checker exceptions
        INVALID_REFERENCE_IN_GROUP_BY("Attribute {0} must appear in the GROUP BY clause or be used in an aggregate function"), AGGREGATE_IN_WHERE_CLAUSE(
                "Aggregate function {0} is not allowed in WHERE clause"),
        
        // DML query checking exceptions
        INCORRECT_PAYLOAD_TYPE("The payload type {0} in {1} does not conform the source type {2}"), MULTI_MODIFICATION_ON_SAME_ATTRIBUTE(
                "Cannot modify the same attribute {0} more than once"), MODIFY_ATTRIBUTE_AND_ITS_ANCESTOR(
                "Cannot modify the attribute {0} and its ancestor {1} simultaneously"), INCORRECT_INSERT_ATTRIBUTE_NUMBER(
                "The number of attributes to be inserted is {0}, but the target {1} tuple has {2} attribute(s)"), INSERT_EXPRESSION_NUMBER_AND_ATTRIBUTE_NOT_MATCH(
                "The number of expressions {0}, the number of attributes to be inserted is {1}  "), INSERT_INTO_NON_COLLECTION_TYPE(
                "Can only insert into collection type, the type of target {0} is {1}"), DELETE_FROM_NON_COLLECTION_TYPE(
                "Can only delete from collection type, the type of target {0} is {1}"), UPDATE_NON_COLLECTION_TYPE(
                "Can only navigate into collection type with UPDATE statement, the type of target {0} is {1}  ");
        
        private static final int  MODULE = 40;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        OldQueryCompilation(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        OldQueryCompilation(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for query parsing.
     */
    public enum QueryParsing implements Messages
    {
        // Query parsing exceptions
        QUERY_PARSE_ERROR("{0}"),
        
        CONSECUTIVE_OFFSET_CLAUSES("Encountered consecutive OFFSET clauses"),
        
        CONSECUTIVE_FETCH_CLAUSES("Encountered consecutive FETCH clauses"),
        
        CONSECUTIVE_ORDER_BY_CLAUSES("Encountered consecutive ORDER BY clauses"),
        
        DEFAULT_VALUE_NOT_SUPPORTED("Default value declaration is only supported in CREATE DATA OBJECT statement");
        
        private static final int  MODULE = 41;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        QueryParsing(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        QueryParsing(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for action compilation.
     * 
     */
    public enum ActionCompilation implements Messages
    {
        UNCHECKED_EXCEPTION("Unexpected error during FPL compilation."),
        
        DATA_SOURCE_ACCESS_EXCEPTION("Error during data source access."),
        
        DUPLICATE_EXCEPTION("Exception \"{0}\" has already been declared."),
        
        UNDECLARED_EXCEPTION("The raised exception \"{0}\" is not declared"),
        
        HANDLED_EXCEPTION_NOT_DECLARED("The handled exception \"{0}\" is not declared"),
        
        UNHANDLED_EXCEPTION("The declared exception \"{0}\" is not handled"),
        
        FUNCTION_REGISTRATION_EXCEPTION("Error during function registration."),
        
        DUPLICATE_PARAMETER_NAME("The parameter name \"{0}\" has already been declared"),
        
        DUPLICATE_VARIABLE_NAME("The variable name \"{0}\" has already been declared"),
        
        INVALID_VARIABLE_DEFAULT_VALUE_TYPE(
                "The declared variable is of type \"{0}\", but the default expression is of type \"{1}\""),
        
        INVALID_RETURN_TYPE("The return type is \"{0}\", instead of the expected \"{1}\""),
        
        INVALID_IF_CONDITION_TYPE("The type of the IF condition \"{0}\" is not boolean"),
        
        UNMATCH_ASSIGNMENT_TYPE("The target in the assignment has type \"{0}\", but the expression is of type \"{1}\""),
        
        UNREACHABLE_STATEMENT("The statement \"{0}\" is not reachable"),
        
        MISSING_RETURN_STATEMENT("The function must return a type \"{0}\""),
        
        MISSING_RETURN_STATEMENT_IN_HANDLER("The handler must return a type \"{0}\"");
        
        private static final int  MODULE = 81;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        ActionCompilation(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        ActionCompilation(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for query compilation.
     */
    public enum QueryCompilation implements Messages
    {
        // Query path exceptions
        INVALID_ACCESS("Invalid access"),
        
        UNKNOWN_ATTRIBUTE("Attribute \"{0}\" cannot be resolved"),
        
        STARTING_WITH_COLLECTION("A collection type can only be the ending step of a query path"),
        
        CROSSING_COLLECTION("Collection-typed attribute \"{0}\" is not the ending step of the query path"),
        
        AMBIGUOUS_QUERY_PATH("Query path \"{0}\" is ambiguous. {1}"),
        
        INVALID_QUERY_PATH_LOCATION("Query path \"{0}\" is in an invalid location"),
        
        NO_ATTRIBUTE_TYPE("Cannot resolve the type for attribute \"{0}\""),
        
        // Function call exceptions
        INVALID_FUNCTION_CALL("Invalid function call"),
        
        NO_FUNCTION_SIGNATURE("No matching signature was found for function call \"{0}\""),
        
        INVALID_TYPE_CONVERION("Cannot convert type \"{0}\" to type \"{1}\""),
        
        INVALID_CASE_CONDITION_TYPE("The return type of the condition of CASE branch \"{0}\" is not boolean"),
        
        INCONSISTENT_CASE_BRANCH_TYPE(
                "The return type of CASE branch \"{0}\" is not the same as the return type of preceding branches"),
        
        NON_SCALAR_CASE_BRANCH_TYPE("The return type of CASE branch \"{0}\" is not scalar"),
        
        // Nested query exceptions
        INVALID_NESTED_QUERY_LOCATION("A nested query cannot appear in this clause"),
        
        // EXISTS exceptions
        INVALID_EXISTS_LOCATION("The EXISTS predicate cannot appear in this clause."),
        
        // SELECT clause exceptions
        DUPLICATE_SELECT_ITEM_ALIAS("The SELECT item alias \"{0}\" is already used by a preceding SELECT item"),
        
        SELECT_STAR_WITH_GROUP_BY("SELECT * clauses are not allowed when the query has a GROUP BY clause"),
        
        // HAVING clause exceptions
        HAVING_CLAUSE_WITHOUT_GROUP_BY("A query cannot have a HAVING clause without a GROUP BY clause"),
        
        // FROM clause exceptions
        DUPLICATE_FROM_ITEM_ALIAS("The alias \"{0}\" is already used by a preceding FROM item"),
        
        INVALID_FROM_ITEM("\"{0}\" is not a valid item in the FROM clause. The FROM clause can only contain collection names"
                + ", subqueries, joins and function calls."),
        
        INVALID_FROM_ITEM_TYPE("The FROM item \"{0}\" is not of collection or tuple type"),
        
        // GROUP BY clause exceptions
        INVALID_GROUP_BY_ITEM("The GROUP BY item \"{0}\" is invalid"),
        
        // OFFSET/FETCH clauses exceptions
        INVALID_OFFSET_PROVIDED_TYPE("Invalid type provided for OFFSET clause"),
        
        INVALID_FETCH_PROVIDED_TYPE("Invalid type provided for FETCH clause"),
        
        // UNION exceptions
        DIFFERENT_UNION_TYPE("Each UNION query must have the same output type"),
        
        // External functions exceptions
        INVALID_EXTERNAL_FUNCTION_LOCATION(
                "An external function call can only appear in the SELECT, FROM, WHERE or HAVING clauses."),
        
        // WITH clause exceptions
        DUPLICATE_WITH_QUERY_NAME("query name \"{0}\" specified more than once."),
        
        // DML exceptions
        DML_INVALID_PROVIDED_TYPE("Invalid type provided for modification target"),
        
        DML_NON_COLLECTION_TARGET_TYPE("The type of modification target is not a collection of tuples"),
        
        DML_UNEQUAL_PROVIDED_TARGET_ATTRS("The number of provided and target attributes for modification target do not match"),
        
        DML_INVALID_TARGET_ATTR("Target attribute \"{0}\" is invalid for modification target query path \"{0}\""),
        
        // Plan parsing exceptions
        LOGICAL_PLAN_PARSE("Error parsing logical plan in XML format"),
        
        PHYSICAL_PLAN_PARSE("Error parsing physical plan in XML format"),
        
        // Query reference checker exceptions
        DUPLICATE_TUPLE_ATTRIBUTE_NAME("The attribute name \"{0}\" is used more than once in the tuple constructor."),
        //
        // DUPLICATE_FROM_ITEM_ALIAS("The alias \"{0}\" for FROM item \"{1}\" is already used by previous FROM item"),
        //
        // AMBIGUOUS_ATTRIBUTE_REFERENCE("The attribute reference \"{0}\" is ambiguous"),
        
        REFERENCE_OUT_OF_SCOPE("The attribute reference \"{0}\" is out of scope"),
        
        NON_EXIST_VARIABLE("The variable \"{0}\" does not exist, should be \"{1}\""),
        
        INVALID_KEY_ON_EXPRESSION("Invalid KEY ON expression \"{0}\", only reference to the output attribute is allowed"),
        
        MISSING_CASE_NAME("The case name of the expression \"{0}\" is missing in the switch constructor \"{1}\""),
        
        MISSING_TUPLE_ATTRIBUTE_NAME("The name of the tuple attribute \"{0}\" is missing in the tuple constructor \"{1}\""),
        
        DUPLICATE_CASE_NAME("The case name \"{0}\" is used more than once in the switch constructor \"{1}\""),
        
        // Query type checker exceptions
        INVALID_ARG_TYPE("Argument of \"{0}\" must be type \"{1}\", not type \"{2}\"  "),
        
        NOT_MATCH_ARG_NUMBER("Function \"{0}\" takes \"{1}\" argument(s), not \"{2}\""),
        
        INVALID_CONDITION_TYPE("Condition \"{0}\" must be type boolean, not type \"{1}\""),
        
        NON_MATCH_TYPE_IN_OUTER_UNION(
                "Non-matching types \"{0}\" and \"{1}\" navigated by \"{2}\" are found in the OUTER UNION query \"{3}\""),
        
        NON_SCALAR_ORDER_BY_TYPE("The ORDER BY item \"{0}\" must be scalar type, not type \"{1}\""),
        
        NON_SCALAR_KEY_ON_TYPE("The KEY ON item \"{0}\" must be scalar type, not type \"{1}\""),
        
        NON_SCALAR_GROUP_BY_TYPE("The GROUP BY item \"{0}\" must be scalar type, not type \"{1}\""),
        
        NON_SCALAR_DISTINCT_TYPE("The DISTINCT keyword is not allowed when one of the SELECT item has a complex type"),
        
        COLLECTION_TYPE_IN_NULL_FUNCTION("Collection type expression \"{0}\" cannot be used in IS NULL function"),
        
        FORWARD_FEATURE_IN_SQL_FUNCTION("The FORWARD construct \"{0}\" could not be used in SQL function \"{1}\""),
        
        // Query key checker exceptions
        INCONSISTENT_UNION_QUERY_PRIMARY_KEY(
                "The primary key(s) \"{0}\" of the UNION query \"{1}\" is different from the primary keys \"{2}\" of another branch"),
        
        NON_SCALAR_TYPE_IN_QUERY_SELECT_ITEM("There must be at least one scalar type SELECT item in query \"{0}\""),
        
        GROUP_BY_ITEM_NOT_DECLARED_IN_SELECT_CLAUSE("The GROUP BY item \"{0}\" is not declared in the SELECT clause"),
        
        FROM_ITEM_KEY_NOT_DECLARED_IN_SELECT_CLAUSE(
                "The primary key \"{1}\" of FROM item's \"{0}\" is not declared in the SELECT clause"),
        
        PRIMARY_KEY_NOT_SPECIFIED("The primary key \"{0}\" is not provided"),
        
        // Query group by checker exceptions
        INVALID_REFERENCE_IN_GROUP_BY("Attribute \"{0}\" must appear in the GROUP BY clause or be used in an aggregate function"),
        
        AGGREGATE_IN_WHERE_CLAUSE("Aggregate function \"{0}\" is not allowed in WHERE clause"),
        
        // DML query checking exceptions
        INCORRECT_PAYLOAD_TYPE("The payload type \"{0}\" in \"{1}\" does not conform the source type \"{2}\""),
        
        MULTI_MODIFICATION_ON_SAME_ATTRIBUTE("Cannot modify the same attribute \"{0}\" more than once"),
        
        MODIFY_ATTRIBUTE_AND_ITS_ANCESTOR("Cannot modify the attribute \"{0}\" and its ancestor \"{1}\" simultaneously"),
        
        INCORRECT_INSERT_ATTRIBUTE_NUMBER(
                "The number of attributes to be inserted is \"{0}\", but the target \"{1}\" tuple has \"{2}\" attribute(s)"),
        
        INSERT_EXPRESSION_NUMBER_AND_ATTRIBUTE_NOT_MATCH(
                "The number of expressions \"{0}\", the number of attributes to be inserted is \"{1}\""),
        
        INSERT_INTO_NON_COLLECTION_TYPE("Can only insert into collection type, the type of target \"{0}\" is \"{1}\""),
        
        DELETE_FROM_NON_COLLECTION_TYPE("Can only delete from collection type, the type of target \"{0}\" is \"{1}\""),
        
        UPDATE_NON_COLLECTION_TYPE(
                "Can only navigate into collection type with UPDATE statement, the type of target \"{0}\" is \"{1}\""),
        
        INPUT_ORDER_INPUT_IS_NOT_ORDERED("A position variable can only be used on an ordered list."),
        
        NESTED_QUERY_IN_OFFSET_FETCH("We do not support nested queries in LIMIT and OFFSET clauses for now."),
        
        NESTED_QUERY_IN_ORDER_BY_WITH_SET_OP(
                "We do not support nested queries in ORDER BY when there is a SET OPERATOR in the query.");
        
        private static final int  MODULE = 41;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        QueryCompilation(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        QueryCompilation(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for query execution.
     */
    public enum ActionInterpretation implements Messages
    {
        NON_EXISTING_INSTRUCTION("The instruction with label \"{0}\" does not exist"),
        
        NO_RETURN_INSTRUCTION("The function ends without a RETURN statement.");
        
        private static final int  MODULE = 82;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        ActionInterpretation(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        ActionInterpretation(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for query execution.
     */
    public enum QueryExecution implements Messages
    {
        // General exceptions
        JDBC_STMT_EXEC_ERROR("Error executing the following statement against the JDBC data source \"{0}\":\n{1}, because {2}"),
        
        NO_FUNCTION_SIGNATURE("No matching signature was found for function call \"{0}\""),
        
        // DDL exceptions
        CREATE_SCHEMA_OBJ_ERROR("Error creating schema object \"{0}\" in data source \"{1}\""),
        
        DROP_SCHEMA_OBJ_ERROR("Error dropping schema object \"{0}\" in data source \"{1}\""),
        
        GET_SCHEMA_OBJ_ERROR("Error accessing \"{0}\" in data source \"{1}\""),
        
        // DML exceptions
        GET_DATA_OBJ_ERROR("Error getting data object \"{0}\" in data source \"{1}\""),
        
        SET_DATA_OBJ_ERROR("Error setting data object \"{0}\" in data source \"{1}\""),
        
        DELETE_DATA_OBJ_ERROR("Error deleting data object \"{0}\" in data source \"{1}\""),
        
        // Index exceptions
        CREATE_INDEX_ERROR("Error creating index \"{0}\" in data source \"{1}\""),
        
        DELETE_INDEX_ERROR("Error deleting index \"{0}\" in data source \"{1}\""),
        
        ACCESS_INDEX_ERROR("Error accessing index \"{0}\" in data source \"{1}\""),
        
        // Pipeline exceptions
        OPEN("Error opening operator \"{0}\""),
        
        NEXT("Error getting the next binding for operator \"{0}\""),
        
        CLOSE("Error closing operator \"{0}\""),
        
        DATA_SOURCE_ACCESS("Error accessing data source \"{0}\""),
        
        // Term evaluation exceptions
        QUERY_PATH_EVAL_ERROR("Error evaluating query path \"{0}\""),
        
        PARAMETER_EVAL_ERROR("Encountered uninstantiated parameter for query path \"{0}\""),
        
        FUNCTION_EVAL_ERROR("Error evaluating function \"{0}\""),
        
        FUNCTION_EVAL_NULL_INPUT_ERROR("Error evaluating function \"{0}\", null value is not an acceptable input"),
        
        // Operator implementation exceptions
        ZERO_DIVISOR_DIV("Encountered a zero divisor in function DIV"),
        
        ZERO_DIVISOR_MOD("Encountered a zero divisor in function MOD"),
        
        NEGATIVE_FETCH("Encountered a negative FETCH value \"{0}\""),
        
        NEGATIVE_OFFSET("Encountered a negative OFFSET value \"{0}\""),
        
        // Transaction exceptions
        INVALID_TRANSACTION_MODE("Error executing \"{0}\" job in the \"{1}\" transaction mode"),
        
        REMOVE_DATASOURCE_WITH_ACTIVE_TRANSACTION(
                "Data source \"{0}\" can't be removed from the unified application state because it has active transactions"),
        
        // Transaction exceptions
        BEGIN("Error beginning a transaction at data source \"{0}\""),
        
        COMMIT("Error committing a transaction at data source \"{0}\""),
        
        ROLLBACK("Error rolling back a transaction at data source \"{0}\""),
        
        // Suspension exceptions
        UNSUPPORTED_SUSPENSION("The suspension is not supported for operator \"{0}\""),
        
        // FPL exceptions
        FPL_FUNCTION_ERROR("Error executing FPL function \"{0}\""),
        
        PAGE_NON_EXISTENT("Page \"{0}\" does not exist"),
        
        // JAVA casting exceptions
        JAVA_CAST_ERROR("Error casting Java value to SQL value"),
        
        AGGREGATE_ON_JAVA_WITH_EMPTY_COLLECTION("Error performing aggregation \"{0}\" on Java reference with empty collection"),
        
        // JSON related exceptions.
        JSON_ACCESS_ERROR("Error accessing JSON value"),
        
        JSON_CAST_ERROR("Error casting JSON value to SQL value"),
        
        // Cast exception
        CAST_ERROR("Error casting value."),
        
        // Remote execution exception
        REMOTE_ERROR("Error executing remote plan \"{0}\""),
        
        // Wrappers exceptions
        WRAPPER_TRANSLATE_ERROR("Error translating the SQL query \"{0}\"");
        
        ;
        
        private static final int  MODULE = 42;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        QueryExecution(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        QueryExecution(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for page configuration parsing.
     * 
     * FIXME: Remove when compilation and runtime projects are completely separated.
     */
    public enum PageConfigCompilation implements Messages
    {
        // General exceptions
        GENERAL_EXCEPTION("Page error {0}"),
        
        IO_EXCEPTION("Error accessing file"),
        
        SAX_EXCEPTION("Error parsing XML file"),
        
        CYCLE_DETECTED("A cycle has been detected in the following set of included files: \"{0}\""),
        
        NOT_FOUND("The page configuration for \"{0}\" could not be found"),
        
        DUPLICATE_NAME("Another element or named expression is already using the name: \"{0}\""),
        
        MULTIPLE_STATEMENTS("Encountered multiple statements when only one is expected in following query: \"{0}\""),
        
        QUERY_PARSE_ERROR("Error parsing query: \"{0}\""),
        
        NON_STATIC_ACTION_INVOCATION_ERROR("Non-static action invocations are not allowed in page configurations: \"{0}\""),
        
        CONFIG_QUERY_COMPILE_ERROR("Error compiling config query: {0}"),
        
        INVALID_TEXT("The \"{0}\" element should not contain any text characters. Encountered unexpected text: \"{1}\""),
        
        STMT_CHILD_ELM(
                "The \"{0}\" statement should not contain any child elements. Encountered an element with tag name: \"{1}\""),
        
        UNSUPPORTED_TAG("Encountered unsupported tag name: \"{0}\""),
        
        DUPLICATE_CASE("The containing switch statement already has another case statement with the option name: \"{0}\""),
        
        STMT_EMTPY_NS(
                "Only the \"statement\" namespace is allowed here. Encountered unexpected empty namespace for element with tag name: \"{0}\""),
        
        STMT_INVALID_NS("Only the \"statement\" namespace is allowed here. Encountered unsupported namespace: \"{0}\""),
        
        SWITCH_TAG(
                "Only \"case\" and \"else\" statements are allowed under a \"switch\" statement. Encountered unexpected tag name: \"{0}\""),
        
        SWITCH_ELSE("The containing \"switch\" statement already has an \"else\" statement. A second \"else\" statement was found"),
        
        SWITCH_TEXT(
                "Only \"case\" and \"else\" statements are allowed under a \"switch\" statement. Encountered unsupported text: \"{0}\""),
        
        UNIT_MISSING("Unit \"{0}/{1}\" does not exist in the unit registry"),
        
        EXP_MULTIPLE("Encountered multiple statements in the expression when only one is expected: \"{0}\""),
        
        EXP_INVALID("Encountered an unsupported expression: \"{0}\""),
        
        EXP_VALUE("Value expression expected: \"{0}\""),
        
        EXP_PARTS(
                "An expression prolog can only have at most 3 tokens: the keyword \"hidden\", then a type, and finally a name: \"{0}\""),
        
        EXP_NAME(
                "Expression name must be alphanumeric, must not be \"hidden\", and must not be the name of a scalar type: \"{0}\""),
        
        EXP_HIDDEN("An expression prolog with 3 tokens must have the keyword \"hidden\" as the first token: \"{0}\""),
        
        EXP_TYPE("Encountered unknown scalar type: \"{0}\""),
        
        EXP_HIDDEN_NAME("A hidden expression must be named: \"{0}\""),
        
        FOR_KEY("A \"for\" statement can contain only one \"key\" statement. Found more than one"),
        
        KEY_TEXT("Only an \"attribute\" statement is allowed under a \"key\" statement. Encountered unsupported text: \"{0}\""),
        
        KEY_TAG("Only an \"attribute\" statement is allowed under a \"key\" statement. Encountered unexpected tag name: \"{0}\""),
        
        ATTR_REQ("The \"{0}\" attribute is required"),
        
        QUERY_DELIM("The \"query\" attribute value must begin with \"{\" and end with \"}\""),
        
        ROOT_ELM("The root element can only be a unit element or an \"escape\" HTML element"),
        
        ROOT_NS(
                "The root element can only be a unit element or an \"escape\" HTML element. Encountered unsupported namespace: \"{0}\""),
        
        PAGE_INPUT_ONE_STMT("Exactly one statement is expected in the page input DDL"),
        
        PAGE_INPUT_QUERY("Error processing page input schema"),
        
        PAGE_INPUT_NAME(" Data object named 'input' expected, but encountered \"{0}\""),
        
        UNIT_NESTED("A unit cannot contain another unit directly. Encountered a unit with tag name: \"{0}\""),
        
        UNIT_CHILD("A unit can have only XML elements as its direct children. Encountered a child element with namespace \"{0}\""),
        
        UNIT_PARSE("Error parsing unit with tag name: \"{0}\""),
        
        XML_LEAF("Leaf XML nodes cannot have both content and non-name attributes"),
        
        XML_SIBILINGS("A unit within an xml element cannot have any other siblings"),
        
        ACTION_NON_EXISTENT("Action \"{0}\" does not exist"),
        
        NS_ERROR("Error parsing namespace: \"{0}\""),
        
        QUERY_MISSING("A query has not been specified in the \"for\" statement"),
        
        KEY_MISSING("A key has not been specified in the \"for\" statement"),
        
        NAME_MISSING("A name has not been specified for the key attribute of the \"for\" statement"),
        
        QUERY_DUPLICATE("A query has already been specified in the \"for\" statement"),
        
        CONDITION_DUPLICATE("A condition has already been specified in the \"case\" statement"),
        
        KEY("The specified key attribute does not exist in the query of the \"for\" statement: \"{0}\""),
        
        QUERY_ATTR_NOT_SUPPORTED(
                "The \"query\" attribute is no longer supported. Please specify the query within the content of the <fstmt:query> child element."),
        
        STYLE_NOT_SUPPORTED(
                "Inline <style> currently causes an error in Internet Explorer, thus should be added as a separate CSS file for now");
        
        private static final int  MODULE = 51;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        PageConfigCompilation(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        PageConfigCompilation(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for ScalarTypeResolver.
     */
    public enum ScalarTypeResolution implements Messages
    {
        // General exceptions
        GENERAL_EXCEPTION("Page error {0}"),
        
        UNKNOWN("Unknown mapping encountered: \"{0}\""),
        
        SCALAR_TYPE("Unable to create source scalar type: \"{0}\""),
        
        CONSTRAINT("Unsupported constraint encountered: \"{0}\"");
        
        private static final int  MODULE = 53;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        ScalarTypeResolution(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        ScalarTypeResolution(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for ConfigQueryBuilder.
     */
    public enum ConfigQueryBuilding implements Messages
    {
        DUPLICATE_NAME(
                "The following name cannot be used because it is already being used by an alias in an enclosing query: \"{0}\""),
        
        NAME_DATA("The following name cannot be used because there exists a data source with the same name: \"{0}\""),
        
        EMPTY_EXPR("Encountered an empty expression being built during query building"),
        
        DIFFERENT_SELECT("Encountered two aliases in the same \"SELECT\" clause with different expressions: \"{0}\", \"{1}\""),
        
        DIFFERENT_TUPLE("Encountered two aliases in the same \"TUPLE\" constructor with different expressions: \"{0}\", \"{1}\"");
        
        private static final int  MODULE = 55;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        ConfigQueryBuilding(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        ConfigQueryBuilding(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for ApplicationBuilder.
     */
    public enum ApplicationBuilding implements Messages
    {
        RUNTIME("Unexpected runtime error encountered"),
        
        ASSERT("Unexpected asssertion error encountered"),
        
        DATA_SOURCE("Error accessing data source"),
        
        QUERY_EXE("Error executing query"),
        
        DUPLICATE_ACTION_NAME("Duplicate action path found: \"{0}\""),
        
        DUPLICATE_FUNCTION_NAME("Duplicate function name found");
        
        private static final int  MODULE = 71;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        ApplicationBuilding(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        ApplicationBuilding(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for Application.
     */
    public enum Application implements Messages
    {
        START_ERROR("Application failed to start: \"{0}\""),
        
        STOP_ERROR("Application failed to stop: \"{0}\""),
        
        ERROR("Application encountered unexpected runtime error: \"{0}\"");
        
        private static final int  MODULE = 72;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        Application(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        Application(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for Realm.
     */
    public enum Realm implements Messages
    {
        INVALID_TAG_NAME("Invalid tag name: \"{0}\". Expected: \"{1}\""),
        
        INVALID_ATTRIBUTE("Invalid attribute value for \"{0}\": \"{1}\"."),
        
        DATA_SOURCE_NOT_FOUND("Data source not found: \"{0}\""),
        
        DATA_OBJECT_NOT_FOUND("Data object not found: \"{0}\""),
        
        DATA_SOURCE_NOT_JDBC("Data source is not JDBC: \"{0}\""),
        
        ATTR_NOT_FOUND("Attribute \"{0}\" not found in data object \"{1}\""),
        
        MULTIPLE_REALMS("More than one realm specified, only one expected."),
        
        IO_ERROR("IOError encountered when processing file."),
        
        DATA_SOURCE_ERROR("Data source exception encountered when accessing realm."),
        
        SQL_ERROR("SQL exception encountered when accessing realm.");
        
        private static final int  MODULE = 73;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        Realm(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        Realm(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for SecurityConstraint.
     */
    public enum SecurityConstraint implements Messages
    {
        INVALID_TAG_NAME("Invalid tag name: \"{0}\". Expected: \"{1}\""),
        
        ONE_ELEMENT_REQUIRED("Exactly one element with tag name \"{0}\" expected."),
        
        IO_ERROR("IOError encountered when processing file."),
        
        ALREADY_SPECIFIED("The program \"{0}\" is already associated with another security constraint."),
        
        PROGRAM_NOT_FOUND("The specified program \"{0}\" could not be found."),
        
        PAGE_NOT_FOUND("The specified page \"{0}\" could not be found."),
        
        ELEMENT_REQUIRED("The element \"{0}\" is required but not found.");
        
        private static final int  MODULE = 74;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        SecurityConstraint(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        SecurityConstraint(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for Authentication.
     */
    public enum Authentication implements Messages
    {
        AUTHENTICATION_FAILED("Authntication failed"),
        
        DATA_SOURCE_NOT_FOUND("Data source not found: \"{0}\""),
        
        DATA_OBJECT_NOT_FOUND("Data object not found: \"{0}\""),
        
        DATA_SOURCE_NOT_JDBC("Data source is not JDBC: \"{0}\""),
        
        ATTR_REQUIRED("The attribute \"{0}\" cannot be null."),
        
        ATTR_NOT_FOUND("Attribute \"{0}\" not found in data object \"{1}\""),
        
        VALUE_REQUIRES_ATTR("A value is specified for \"{0}\", but the \"{1}\" attribute is not specified."),
        
        INSERT_FAILED("Insert into table \"{0}\" failed."),
        
        USER_NOT_FOUND("Username not found: \"{0}\"."),
        
        TOKEN_NOT_FOUND("Token not found: \"{0}\"."),
        
        OUT_OF_USES("The number of times then the token has been used has exceeded the allowed limit."),
        
        EXPIRED("The token has expired."),
        
        SQL_ERROR("SQL exception encountered when accessing table."),
        
        DATA_SOURCE_ERROR("Data source exception encountered when accessing table.");
        
        private static final int  MODULE = 75;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        Authentication(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        Authentication(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for Registration.
     */
    public enum Registration implements Messages
    {
        
        SQL_ERROR("SQL exception encountered when accessing table."),
        
        DATA_SOURCE_ERROR("Data source exception encountered when accessing table."),
        
        ACTIVE_USER("The user has already completed registration: {0}."),
        
        REGISTERED_NOT_ACTIVE("The user has already registered but has not completed registration: {0}."),
        
        ACTIVATION_TOKEN_NOT_FOUND("The specified activation token does not exist."),
        
        INVITATION_TOKEN_NOT_FOUND("The specified invitation token does not exist."),
        
        PASSWORD_RESET_TOKEN_NOT_FOUND("The specified password reset token does not exist."),
        
        INACTIVE_USER("The user does not exist or is inactive: {0}."), ;
        
        private static final int  MODULE = 76;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        Registration(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        Registration(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * A list of error messages for other exceptions.
     */
    public enum Other implements Messages
    {
        PROGRAM_INVOCATION("{0}"),
        
        SERVICE_INITIALIZATION("{0}"),
        
        XML_PARSER("{0}"),
        
        URL_PATTERN("{0}"),
        
        REFRESH_REQUEST_PARSE("{0}");
        
        private static final int  MODULE = 00;
        
        private final int         m_code;
        
        private final MessageType m_type;
        
        private final String      m_message;
        
        /**
         * Constructor.
         * 
         * @param message
         *            the message.
         */
        Other(String message)
        {
            this(MessageType.ERROR, message);
        }
        
        /**
         * Constructor.
         * 
         * @param type
         *            the type of the message.
         * @param message
         *            the message.
         */
        Other(MessageType type, String message)
        {
            m_code = MODULE;
            m_type = type;
            m_message = message;
        }
        
        @Override
        public int getMessageCode()
        {
            return m_code;
        }
        
        @Override
        public String getMessageId()
        {
            return name();
        }
        
        @Override
        public MessageType getMessageType()
        {
            return m_type;
        }
        
        @Override
        public String getMessage(Object... params)
        {
            return messageFormat(m_message, params);
        }
    }
    
    /**
     * Instantiates a message template given the provided parameters and returns a string.
     * 
     * @param pattern
     *            the message template.
     * @param arguments
     *            the provided parameters.
     * @return an instantiated message template.
     */
    private static String messageFormat(String pattern, Object... arguments)
    {
        String s = pattern;
        int i = 0;
        while (i < arguments.length)
        {
            String delimiter = "{" + i + "}";
            while (s.contains(delimiter))
            {
                s = s.replace(delimiter, String.valueOf(arguments[i]));
            }
            i++;
        }
        return s;
    }
    
}
