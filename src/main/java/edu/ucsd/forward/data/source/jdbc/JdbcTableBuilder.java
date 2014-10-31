/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.constraint.AutoIncrementConstraint;
import edu.ucsd.forward.data.constraint.Constraint;
import edu.ucsd.forward.data.constraint.LocalPrimaryKeyConstraint;
import edu.ucsd.forward.data.constraint.NonNullConstraint;
import edu.ucsd.forward.data.index.IndexDeclaration;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.source.DataSourceTransaction.TransactionState;
import edu.ucsd.forward.data.source.jdbc.model.Column;
import edu.ucsd.forward.data.source.jdbc.model.ColumnHandle;
import edu.ucsd.forward.data.source.jdbc.model.Index;
import edu.ucsd.forward.data.source.jdbc.model.SqlAutoIncrementConstraint;
import edu.ucsd.forward.data.source.jdbc.model.SqlConstraint;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;
import edu.ucsd.forward.data.source.jdbc.model.SqlPrimaryKeyConstraint;
import edu.ucsd.forward.data.source.jdbc.model.Table;
import edu.ucsd.forward.data.source.jdbc.model.TableHandle;
import edu.ucsd.forward.data.source.jdbc.model.Table.TableScope;
import edu.ucsd.forward.data.source.jdbc.statement.AnalyzeStatement;
import edu.ucsd.forward.data.source.jdbc.statement.CreateAutoIncrementColumnStatement;
import edu.ucsd.forward.data.source.jdbc.statement.CreateIndexStatement;
import edu.ucsd.forward.data.source.jdbc.statement.CreatePrimaryKeyStatement;
import edu.ucsd.forward.data.source.jdbc.statement.CreateTableStatement;
import edu.ucsd.forward.data.source.jdbc.statement.DropIndexStatement;
import edu.ucsd.forward.data.source.jdbc.statement.DropTableStatement;
import edu.ucsd.forward.data.source.jdbc.statement.InsertStatement;
import edu.ucsd.forward.data.source.jdbc.statement.JdbcStatement;
import edu.ucsd.forward.data.source.jdbc.statement.TruncateStatement;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;

/**
 * Builder of JDBC tables.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public final class JdbcTableBuilder
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(JdbcTableBuilder.class);
    
    /**
     * Private constructor.
     */
    private JdbcTableBuilder()
    {
        
    }
    
    /**
     * Creates a SQL table for a given schema tree.
     * 
     * @param table_name
     *            the table name
     * @param schema_obj
     *            the schema object
     * @param transaction
     *            the transaction to execute statements on.
     * @return table specification
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    protected static Table createTable(String table_name, SchemaObject schema_obj, JdbcTransaction transaction)
            throws QueryExecutionException
    {
        assert (transaction != null && transaction.getState() == TransactionState.ACTIVE);
        
        SchemaTree schema_tree = schema_obj.getSchemaTree();
        CollectionType coll_type = (CollectionType) schema_tree.getRootType();
        
        // Builds the SQL table
        JdbcDataSource data_source = transaction.getDataSource();
        String schema = data_source.getMetaData().getSchemaName();
        Table table = null;
        switch (schema_obj.getScope())
        {
            case PERSISTENT:
                table = Table.create(TableScope.PERSISTENT, schema, table_name, coll_type);
                break;
            case TEMPORARY:
                table = Table.create(TableScope.LOCAL_TEMPORARY, schema, table_name, coll_type);
                break;
        }
        
        addIdentifiers(transaction.getDataSource().getIdentifierDictionary(), table);
        
        // Construct the DDL statement
        CreateTableStatement create_table_statement = new CreateTableStatement(table);
        
        // Execute the statement
        JdbcExecution.run(transaction, create_table_statement);
        
        // Analyzes the table to update statistics
        analyze(table, transaction);
        
        // Builds the SQL constraints
        for (Constraint constraint : schema_tree.getConstraints())
        {
            SqlConstraint sql_constraint = newSqlConstraint(constraint, table);
            if (sql_constraint == null) continue;
            
            // FIXME: the SQL constraint should be added to the schema or table
            
            JdbcStatement s = newSqlConstraintCreateStatement(sql_constraint);
            JdbcExecution.run(transaction, s);
        }
        
        // FIXME Generalize!
        // Build the LocalPrimaryKeyConstraint
        if (coll_type.hasLocalPrimaryKeyConstraint())
        {
            SqlConstraint sql_constraint = newSqlConstraint(coll_type.getLocalPrimaryKeyConstraint(), table);
            
            // FIXME: the SQL constraint should be added to the schema or table
            
            JdbcStatement s = newSqlConstraintCreateStatement(sql_constraint);
            JdbcExecution.run(transaction, s);
        }
        
        return table;
    }
    
    /**
     * Creates a SQL index.
     * 
     * @param declaration
     *            the index declaration.
     * @param table
     *            the table to build the index on.
     * @param transaction
     *            the transaction to execute statements on.
     * @return index specification
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    protected static Index createIndex(IndexDeclaration declaration, Table table, JdbcTransaction transaction)
            throws QueryExecutionException
    {
        assert (transaction != null && transaction.getState() == TransactionState.ACTIVE);
        SqlIdentifierDictionary dictionary = transaction.getDataSource().getIdentifierDictionary();
        dictionary.getTruncatedSystemString(declaration.getName());
        
        Index index = Index.create(declaration, table);
        // Construct the DDL statement
        CreateIndexStatement create_index_statement = new CreateIndexStatement(index);
        
        // Execute the statement
        JdbcExecution.run(transaction, create_index_statement);
        
        // Analyzes the table to update statistics
        analyze(table, transaction);
        
        return index;
    }
    
    /**
     * Analyzes a SQL table to update the statistics. This method is only applicable to PostgreSQL database.
     * 
     * @param table
     *            the table to analyze
     * @param transaction
     *            the transaction that the statement is on
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    protected static void analyze(Table table, JdbcTransaction transaction) throws QueryExecutionException
    {
        assert table != null;
        assert (transaction != null && transaction.getState() == TransactionState.ACTIVE);
        
        // NOTE: POSTGRES ONLY!
        if (transaction.getDataSource().getSqlDialect() == SqlDialect.POSTGRESQL)
        {
            AnalyzeStatement statement = new AnalyzeStatement(new TableHandle(table));
            JdbcExecution.run(transaction, statement);
        }
    }
    
    /**
     * Drops a SQL table.
     * 
     * @param table
     *            the table to drop
     * @param transaction
     *            the transaction that the statement is on
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    protected static void dropTable(Table table, JdbcTransaction transaction) throws QueryExecutionException
    {
        assert table != null;
        assert (transaction != null && transaction.getState() == TransactionState.ACTIVE);
        
        DropTableStatement statement = new DropTableStatement(table);
        JdbcExecution.run(transaction, statement);
    }
    
    /**
     * Drops a SQL index.
     * 
     * @param index
     *            the index to drop
     * @param transaction
     *            the transaction that the statement is on
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    protected static void dropIndex(Index index, JdbcTransaction transaction) throws QueryExecutionException
    {
        assert index != null;
        assert (transaction != null && transaction.getState() == TransactionState.ACTIVE);
        
        DropIndexStatement statement = new DropIndexStatement(index);
        JdbcExecution.run(transaction, statement);
    }
    
    /**
     * Adds to a dictionary long SQL identifiers that are used in a table.
     * 
     * @param dictionary
     *            - the dictionary.
     * @param table
     *            - the table.
     */
    private static void addIdentifiers(SqlIdentifierDictionary dictionary, Table table)
    {
        // getTruncatedSystemString() will add the identifier names as a side-effect
        
        dictionary.getTruncatedSystemString(table.getName());
        
        for (Column column : table.getColumns())
        {
            dictionary.getTruncatedSystemString(column.getName());
        }
    }
    
    /**
     * Populates the tuples into an existing table.
     * 
     * @param transaction
     *            the transaction to execute statements on.
     * @param tuples
     *            the tuples holding the data.
     * @param table
     *            the SQL table.
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    protected static void populate(JdbcTransaction transaction, List<TupleValue> tuples, Table table)
            throws QueryExecutionException
    {
        assert tuples != null;
        assert table != null;
        assert (transaction != null && transaction.getState() == TransactionState.ACTIVE);
        
        List<ColumnHandle> column_handles = new ArrayList<ColumnHandle>();
        for (Column column : table.getColumns())
        {
            column_handles.add(new ColumnHandle(column));
        }
        
        InsertStatement statement = new InsertStatement(new TableHandle(table), column_handles);
        JdbcExecution execution = new JdbcExecution(transaction, statement);
        
        // Add to the batch
        for (TupleValue tuple : tuples)
        {
            // Prepare the arguments
            Value[] arguments = new Value[column_handles.size()];
            for (int i = 0; i < column_handles.size(); i++)
            {
                Value argument = tuple.getAttribute(column_handles.get(i).getName());
                assert argument != null;
                arguments[i] = argument;
            }
            execution.addBatch(arguments);
        }
        
        // Execute the batch
        execution.executeBatch();
        execution.end();
        
        // Analyzes the table to update statistics
        analyze(table, transaction);
    }
    
    /**
     * Truncates a SQL table.
     * 
     * @param transaction
     *            the transaction to execute statements on.
     * @param table
     *            the SQL table.
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    protected static void truncate(JdbcTransaction transaction, Table table) throws QueryExecutionException
    {
        assert table != null;
        assert (transaction != null && transaction.getState() == TransactionState.ACTIVE);
        
        TruncateStatement statement = new TruncateStatement(new TableHandle(table));
        JdbcExecution.run(transaction, statement);
        
        // Analyzes the table to update statistics
        analyze(table, transaction);
    }
    
    /**
     * Constructs the corresponding SQL constraint for a constraint.
     * 
     * @param constraint
     *            - the constraint.
     * @param table
     *            the table that the constraint is associated with
     * @return the SQL constraint.
     */
    private static SqlConstraint newSqlConstraint(Constraint constraint, Table table)
    {
        // FIXME: Handle other constraints
        if (constraint instanceof LocalPrimaryKeyConstraint)
        {
            LocalPrimaryKeyConstraint local_key_constraint = (LocalPrimaryKeyConstraint) constraint;
            
            List<Column> columns = new ArrayList<Column>();
            for (ScalarType attribute : local_key_constraint.getAttributes())
            {
                Type p = attribute.getParent();
                assert(p instanceof TupleType);
                TupleType parent = (TupleType) p;
                String column_name = parent.getAttributeName(attribute);
                columns.add(table.getColumn(column_name));
            }
            return new SqlPrimaryKeyConstraint(table, columns);
        }
        else if (constraint instanceof AutoIncrementConstraint)
        {
            AutoIncrementConstraint auto_increment_constraint = (AutoIncrementConstraint) constraint;
            ScalarType attribute = auto_increment_constraint.getAttribute();
            
            Type p = attribute.getParent();
            assert(p instanceof TupleType);
            TupleType parent = (TupleType) p;
            String column_name = parent.getAttributeName(attribute);
            Column column = table.getColumn(column_name);
            
            return new SqlAutoIncrementConstraint(table, column);
        }
        else
        {
            assert constraint instanceof NonNullConstraint;
            return null;
        }
    }
    
    /**
     * Constructs the DDL statement for a SQL constraint.
     * 
     * @param sql_constraint
     *            - the SQL constraint.
     * @return the DDL statement.
     */
    private static JdbcStatement newSqlConstraintCreateStatement(SqlConstraint sql_constraint)
    {
        if (sql_constraint instanceof SqlPrimaryKeyConstraint)
        {
            return new CreatePrimaryKeyStatement((SqlPrimaryKeyConstraint) sql_constraint);
        }
        else
        {
            assert (sql_constraint instanceof SqlAutoIncrementConstraint);
            return new CreateAutoIncrementColumnStatement((SqlAutoIncrementConstraint) sql_constraint);
        }
        
    }
}
