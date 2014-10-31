/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.postgresql;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

import org.postgresql.core.BaseConnection;
import org.postgresql.core.BaseStatement;
import org.postgresql.core.Field;
import org.postgresql.core.Oid;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.constraint.ForeignKeyConstraint;
import edu.ucsd.forward.data.constraint.LocalPrimaryKeyConstraint;
import edu.ucsd.forward.data.constraint.NonNullConstraint;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.source.SchemaObject.SchemaObjectScope;
import edu.ucsd.forward.data.source.SchemaObjectHandle;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSource;
import edu.ucsd.forward.data.source.jdbc.JdbcExclusion;
import edu.ucsd.forward.data.source.jdbc.JdbcIntrospector;
import edu.ucsd.forward.data.source.jdbc.model.SqlTypeConverter;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.NullType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.exception.ExceptionMessages.DataSource;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.function.FunctionSignature;
import edu.ucsd.forward.query.function.external.ExternalFunction;
import edu.ucsd.forward.query.logical.CardinalityEstimate;
import edu.ucsd.forward.query.logical.term.QueryPath;

/**
 * Introspect an existing database by reading the JDBC MetaData and creates a schema object for each existing SQL table.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public class JdbcIntrospectorPostgreSql implements JdbcIntrospector
{
    private static final Logger log = Logger.getLogger(JdbcIntrospectorPostgreSql.class);
    
    private Connection          m_connection;
    
    private JdbcDataSource      m_data_source;
    
    private DatabaseMetaData    m_jdbc_metadata;
    
    private int                 m_void_type_id;
    
    /**
     * Constructs with the JDBC data source.
     * 
     * @param data_source
     *            the JDBC data source
     * @throws DataSourceException
     *             if a connection cannot be obtained.
     */
    public JdbcIntrospectorPostgreSql(JdbcDataSource data_source) throws DataSourceException
    {
        assert data_source != null;
        m_data_source = data_source;
    }
    
    /**
     * Opens a direct connection to the JDBC data source, checks that the schema exists, and sets the metadata.
     * 
     * @throws DataSourceException
     *             if there is an error connecting to the JDBC data source.
     */
    private void init() throws DataSourceException
    {
        // Open a direct connection, not one from the connection pool.
        m_connection = m_data_source.getDirectConnection();
        
        String schema = m_data_source.getMetaData().getSchemaName();
        
        try
        {
            m_jdbc_metadata = m_connection.getMetaData();
        }
        catch (SQLException ex)
        {
            throw new DataSourceException(ExceptionMessages.DataSource.JDBC_SCHEMA_METADATA, ex, schema,
                                          m_data_source.getMetaData().getName());
        }
        
        // Check if the schema exists
        try
        {
            boolean exist = false;
            ResultSet rs = m_jdbc_metadata.getSchemas();
            while (rs.next())
            {
                if (rs.getString("TABLE_SCHEM").equals(schema))
                {
                    exist = true;
                    break;
                }
            }
            
            if (!exist)
            {
                throw new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_JDBC_SCHEMA, schema,
                                              m_data_source.getMetaData().getName());
            }
        }
        catch (SQLException ex)
        {
            throw new DataSourceException(ExceptionMessages.DataSource.JDBC_SCHEMA_METADATA, ex, schema,
                                          m_data_source.getMetaData().getName());
        }
    }
    
    @Override
    public Collection<SchemaObject> introspectSchemas(List<JdbcExclusion> exclusions)
            throws DataSourceException, QueryExecutionException
    {
        init();
        
        String schema = m_data_source.getMetaData().getSchemaName();
        
        // Introspect tables and views
        List<SchemaObject> result = new ArrayList<SchemaObject>();
        // The types of the table to introspect
        String[] types = { "TABLE", "VIEW" };
        try
        {
            ResultSet rs = m_jdbc_metadata.getTables(m_connection.getCatalog(), schema, null, types);
            while (rs.next())
            {
                // Get the table name
                String name = rs.getString("TABLE_NAME");
                // Construct the schema
                SchemaTree schema_tree = this.getSchemaTree(name);
                // Create primary key constraint
                this.getPrimaryKeys(name, (CollectionType) schema_tree.getRootType());
                // Create new schema object, all the objects in JDBC data source are estimated as large
                SchemaObject object = new SchemaObject(name, SchemaObjectScope.PERSISTENT, schema_tree,
                                                       CardinalityEstimate.Size.LARGE);
                
                assert (TypeUtil.checkConstraintConsistent(schema_tree));
                
                result.add(object);
                
                m_data_source.importSchemaObject(object);
            }
            
            // Iterate over them one more time to get the foreign keys
            rs.beforeFirst();
            while (rs.next())
            {
                // Get the table name
                String name = rs.getString("TABLE_NAME");
                // Create foreign key constraint
                this.getForeignKeys(name, exclusions);
            }
            
            log.debug("");
            log.debug("DatabaseProductName: " + m_jdbc_metadata.getDatabaseProductName());
            log.debug("DatabaseProductVersion: " + m_jdbc_metadata.getDatabaseProductVersion());
            log.debug("");
        }
        catch (SQLException ex)
        {
            throw new DataSourceException(ExceptionMessages.DataSource.JDBC_SCHEMA_METADATA, ex, schema,
                                          m_data_source.getMetaData().getName());
        }
        
        // Close the direct connection
        try
        {
            m_connection.close();
        }
        catch (SQLException e)
        {
            // FIXME Throw a DataSourceException
            throw new AssertionError(e);
        }
        
        return result;
    }
    
    @Override
    public Collection<ExternalFunction> introspectFunctions(List<JdbcExclusion> exclusions) throws DataSourceException
    {
        init();
        
        String schema = m_data_source.getMetaData().getSchemaName();
        
        // Find the 'void' type id
        try
        {
            ResultSet void_rs = m_connection.createStatement().executeQuery("SELECT oid FROM pg_type WHERE typname = 'void'");
            void_rs.next();
            m_void_type_id = void_rs.getInt(1);
            void_rs.close();
        }
        catch (SQLException ex)
        {
            // FIXME Throw a data source exception
            assert (false);
        }
        
        Map<String, ExternalFunction> result = new HashMap<String, ExternalFunction>();
        try
        {
            ResultSet sp_rs = this.getProcedures(m_connection.getCatalog(), schema, null);
            while (sp_rs.next())
            {
                // Get the external function name
                String name = sp_rs.getString("PROCEDURE_NAME");
                
                log.debug("");
                log.debug("PROCEDURE_NAME: " + m_data_source.getMetaData().getName() + QueryPath.PATH_SEPARATOR + name);
                log.debug("PROCEDURE_TYPE: " + sp_rs.getShort("PROCEDURE_TYPE"));
                
                String specific_name = sp_rs.getString("SPECIFIC_NAME");
                
                Type return_type = null;
                List<SchemaObject> arguments = new ArrayList<SchemaObject>();
                boolean valid = true;
                ResultSet rs = this.getProcedureColumns(m_connection.getCatalog(), schema, specific_name, null);
                while (rs.next())
                {
                    int column_type = rs.getShort("COLUMN_TYPE");
                    Class<? extends ScalarType> column_type_class = SqlTypeConverter.getType(rs.getInt("DATA_TYPE"));
                    String column_name = rs.getString("COLUMN_NAME");
                    
                    // Skip stored procedures with unknown/composite types
                    if (column_type_class == null)
                    {
                        log.debug("\t UNKNOWN : " + column_name + " ("
                                + SqlTypeConverter.getSqlTypeNameFromTypeCode(rs.getInt("DATA_TYPE")) + ")");
                        valid = false;
                        break;
                    }
                    
                    String trace = null;
                    
                    Type attr_type;
                    SchemaObject object;
                    TupleType return_tuple;
                    switch (column_type)
                    {
                        case DatabaseMetaData.procedureColumnIn:
                            trace = "\t IN: ";
                            
                            // Create new schema object for the argument
                            attr_type = TypeUtil.cloneNoParent(TypeEnum.get(column_type_class));
                            object = new SchemaObject(column_name, SchemaObjectScope.PERSISTENT, new SchemaTree(attr_type),
                                                      CardinalityEstimate.Size.UNKNOWN);
                            arguments.add(object);
                            
                            break;
                        case DatabaseMetaData.procedureColumnOut:
                            trace = "\t OUT: ";
                            
                            return_tuple = null;
                            // The return type is a scalar previously encountered as DatabaseMetaData.procedureColumnReturn
                            if (return_type instanceof ScalarType)
                            {
                                break;
                            }
                            else if (return_type instanceof CollectionType)
                            {
                                // Return type is a collection type previously encountered as DatabaseMetaData.procedureColumnResult
                                return_tuple = ((CollectionType) return_type).getTupleType();
                            }
                            else if (return_type == null)
                            { // The return type is a tuple
                                return_type = return_tuple = new TupleType();
                            }
                            else
                            {
                                return_tuple = (TupleType) return_type;
                            }
                            
                            attr_type = TypeUtil.cloneNoParent(TypeEnum.get(column_type_class));
                            return_tuple.setAttribute(column_name, attr_type);
                            
                            break;
                        case DatabaseMetaData.procedureColumnInOut:
                            trace = "\t INOUT: ";
                            
                            // Create new schema object for the argument
                            attr_type = TypeUtil.cloneNoParent(TypeEnum.get(column_type_class));
                            object = new SchemaObject(column_name, SchemaObjectScope.PERSISTENT, new SchemaTree(attr_type),
                                                      CardinalityEstimate.Size.UNKNOWN);
                            arguments.add(object);
                            
                            return_tuple = null;
                            // The return type is a scalar previously encountered as DatabaseMetaData.procedureColumnReturn
                            if (return_type instanceof ScalarType)
                            {
                                break;
                            }
                            else if (return_type instanceof CollectionType)
                            {
                                // Return type is a collection type previously encountered as DatabaseMetaData.procedureColumnResult
                                return_tuple = ((CollectionType) return_type).getTupleType();
                            }
                            else if (return_type == null)
                            { // The return type is a tuple
                                return_type = return_tuple = new TupleType();
                            }
                            else
                            {
                                return_tuple = (TupleType) return_type;
                            }
                            
                            attr_type = TypeUtil.cloneNoParent(TypeEnum.get(column_type_class));
                            return_tuple.setAttribute(column_name, attr_type);
                            
                            break;
                        case DatabaseMetaData.procedureColumnReturn:
                            trace = "\t RETURN: ";
                            
                            // The return type is scalar
                            assert (return_type == null);
                            return_type = TypeUtil.cloneNoParent(TypeEnum.get(column_type_class));
                            
                            break;
                        case DatabaseMetaData.procedureColumnResult:
                            trace = "\t RESULT: ";
                            
                            // The return type is a collection
                            if (return_type == null)
                            {
                                return_type = new CollectionType();
                            }
                            
                            TupleType tuple_type = ((CollectionType) return_type).getTupleType();
                            attr_type = TypeUtil.cloneNoParent(TypeEnum.get(column_type_class));
                            if (column_name != null)
                            {
                                tuple_type.setAttribute(column_name, attr_type);
                            }
                            
                            break;
                        // TODO : Remove this case since we break above.
                        case DatabaseMetaData.procedureColumnUnknown:
                            trace = "\t UNKNOWN: ";
                            
                            // Skip stored procedures with unknown/composite types
                            valid = false;
                            
                            break;
                    }
                    
                    log.debug(trace + column_name + " (" + column_type_class.getSimpleName() + ")");
                    
                }
                
                if (valid)
                {
                    // In case of void functions, the return type is a null type.
                    if (return_type == null)
                    {
                        return_type = new NullType();
                    }
                    
                    ExternalFunction function = result.get(name);
                    
                    if (function == null)
                    {
                        // Construct the external function and add it to the data source
                        function = new ExternalFunction(m_data_source.getMetaData().getName(), name);
                        result.put(name, function);
                        m_data_source.addExternalFunction(function);
                    }
                    
                    FunctionSignature signature = new FunctionSignature("sig_" + function.getFunctionSignatures().size(),
                                                                        return_type);
                    for (SchemaObject arg : arguments)
                    {
                        signature.addArgument(arg.getName(), arg.getSchemaTree().getRootType());
                    }
                    
                    function.addFunctionSignature(signature);
                }
                else
                {
                    log.debug("\t NOT IMPORTED");
                }
            }
            
            // log.debug("");
            // log.debug("NumericFunctions: " + m_jdbc_metadata.getNumericFunctions());
            // log.debug("StringFunctions: " + m_jdbc_metadata.getStringFunctions());
            // log.debug("TimeDateFunctions: " + m_jdbc_metadata.getTimeDateFunctions());
            // log.debug("");
        }
        catch (SQLException ex)
        {
            throw new DataSourceException(ExceptionMessages.DataSource.JDBC_SCHEMA_METADATA, ex, schema,
                                          m_data_source.getMetaData().getName());
        }
        
        try
        {
            m_connection.close();
        }
        catch (SQLException e)
        {
            // FIXME Throw a DataSourceException
            new AssertionError(e);
        }
        
        return result.values();
    }
    
    /**
     * Gets the primary key of the given collection type by introspecting the metadata.
     * 
     * @param name
     *            the name of the table.
     * @param collection_type
     *            the collection type constructed from the table.
     * @throws DataSourceException
     *             when encounters error while reading the metadata
     */
    private void getPrimaryKeys(String name, CollectionType collection_type) throws DataSourceException
    {
        try
        {
            ResultSet keys_rs = m_jdbc_metadata.getPrimaryKeys(m_connection.getCatalog(),
                                                               m_data_source.getMetaData().getSchemaName(), name);
            List<Type> keys = new ArrayList<Type>();
            while (keys_rs.next())
            {
                String attribute = keys_rs.getString("COLUMN_NAME");
                Type type = collection_type.getTupleType().getAttribute(attribute);
                if (type == null)
                {
                    log.debug("Skipped adding primary key constraint to \"" + name + "\" because key \"" + attribute
                            + "\" is not supported.");
                    // If at least one key is not supported, then we shouldn't add the constraint period.
                    return;
                }
                keys.add((ScalarType) type);
            }
            if (!keys.isEmpty())
            {
                new LocalPrimaryKeyConstraint(collection_type, keys);
            }
        }
        catch (SQLException ex)
        {
            throw new DataSourceException(ExceptionMessages.DataSource.JDBC_TABLE_METADATA, ex, name,
                                          m_data_source.getMetaData().getName());
        }
    }
    
    /**
     * Gets the foreign keys of the given collection type by introspecting the metadata.
     * 
     * @param name
     *            the name of the table.
     * @param exclusions
     *            what to exclude when importing an existing relational schema.
     * @throws DataSourceException
     *             when encounters error while reading the metadata
     */
    private void getForeignKeys(String name, List<JdbcExclusion> exclusions) throws DataSourceException
    {
        try
        {
            ResultSet keys_rs = m_jdbc_metadata.getImportedKeys(m_connection.getCatalog(),
                                                                m_data_source.getMetaData().getSchemaName(), name);
            
            String fk_name = null;
            SchemaObjectHandle foreign_handle = null;
            SchemaObjectHandle primary_handle = null;
            CollectionType foreign_type = null;
            CollectionType primary_type = null;
            List<ScalarType> foreign_attrs = new ArrayList<ScalarType>();
            List<ScalarType> primary_attrs = new ArrayList<ScalarType>();
            while (keys_rs.next())
            {
                if (!keys_rs.getString("FK_NAME").equals(fk_name))
                {
                    boolean skip = false;
                    for (JdbcExclusion exclusion : exclusions)
                    {
                        if (exclusion.getType() == JdbcExclusion.Type.FOREIGN_KEY
                                && exclusion.getProperties().getProperty("table").equals(name)
                                && exclusion.getProperties().getProperty("name").equals(keys_rs.getString("FK_NAME")))
                        {
                            skip = true;
                            break;
                        }
                    }
                    if (skip) continue;
                    
                    // It's not the first foreign key constraint encountered.
                    if (fk_name != null)
                    {
                        new ForeignKeyConstraint(foreign_handle, foreign_type, foreign_attrs, primary_handle, primary_type,
                                                 primary_attrs);
                    }
                    
                    fk_name = keys_rs.getString("FK_NAME");
                    String fk_table_name = keys_rs.getString("FKTABLE_NAME");
                    String pk_table_name = keys_rs.getString("PKTABLE_NAME");
                    foreign_handle = new SchemaObjectHandle(m_data_source.getMetaData().getName(), fk_table_name);
                    primary_handle = new SchemaObjectHandle(m_data_source.getMetaData().getName(), pk_table_name);
                    try
                    {
                        foreign_type = (CollectionType) m_data_source.getSchemaObject(fk_table_name).getSchemaTree().getRootType();
                        primary_type = (CollectionType) m_data_source.getSchemaObject(pk_table_name).getSchemaTree().getRootType();
                    }
                    catch (QueryExecutionException e)
                    {
                        throw new AssertionError(e);
                    }
                    foreign_attrs.clear();
                    primary_attrs.clear();
                }
                Type foreign_attr = foreign_type.getTupleType().getAttribute(keys_rs.getString("FKCOLUMN_NAME"));
                Type primary_attr = primary_type.getTupleType().getAttribute(keys_rs.getString("PKCOLUMN_NAME"));
                if (foreign_attr == null || primary_attr == null)
                {
                    log.debug("Skipped adding foreign key constraint to table \"" + name + "\" because fkey \""
                            + keys_rs.getString("FKCOLUMN_NAME") + "\" is not supported.");
                    // If at least one key is not supported, then we shouldn't add the constraint period.
                    return;
                }
                foreign_attrs.add((ScalarType) foreign_attr);
                primary_attrs.add((ScalarType) primary_attr);
            }
            
            // Add the last foreign key constraint encountered, if any.
            if (fk_name != null)
            {
                new ForeignKeyConstraint(foreign_handle, foreign_type, foreign_attrs, primary_handle, primary_type, primary_attrs);
            }
        }
        catch (SQLException ex)
        {
            throw new DataSourceException(ExceptionMessages.DataSource.JDBC_TABLE_METADATA, ex, name,
                                          m_data_source.getMetaData().getName());
        }
    }
    
    /**
     * Creates the schema tree representing the SQL table by the specified name.
     * 
     * @param name
     *            the specified table name
     * @return the created schema tree for the specified SQL table
     * @throws DataSourceException
     *             when encounters error while reading the metadata
     */
    private SchemaTree getSchemaTree(String name) throws DataSourceException
    {
        TupleType tuple_type = new TupleType();
        SchemaTree schema_tree = new SchemaTree(new CollectionType(tuple_type));
        try
        {
            ResultSet columns = m_jdbc_metadata.getColumns(m_connection.getCatalog(), m_data_source.getMetaData().getSchemaName(),
                                                           name, null);
            
            log.debug("");
            log.debug("TABLE_NAME: " + m_data_source.getMetaData().getName() + "." + name);
            while (columns.next())
            {
                int data_type = columns.getInt("DATA_TYPE");
                Class<? extends ScalarType> type_class = SqlTypeConverter.getType(data_type);
                
                if (type_class != null)
                {
                    log.debug("\t" + "COLUMN_NAME: " + columns.getString("COLUMN_NAME") + " (" + type_class.getSimpleName() + ")");
                }
                else
                {
                    // Skip attributes of unsupported type
                    log.debug("\t" + "COLUMN_NAME: " + columns.getString("COLUMN_NAME") + " (skipped due to unsupported type "
                            + SqlTypeConverter.getSqlTypeNameFromTypeCode(data_type) + ")");
                    continue;
                }
                
                ScalarType type = (ScalarType) TypeUtil.clone(TypeEnum.get(type_class));
                String column_name = columns.getString("COLUMN_NAME");
                tuple_type.setAttribute(column_name, type);
                // Check non-null constraint
                int nullable = columns.getInt("NULLABLE");
                if (nullable == DatabaseMetaData.columnNoNulls)
                {
                    new NonNullConstraint(type);
                }
                // FIXME Check auto-increment constraint
            }
        }
        catch (SQLException ex)
        {
            throw new DataSourceException(ExceptionMessages.DataSource.JDBC_TABLE_METADATA, ex, name,
                                          m_data_source.getMetaData().getName());
        }
        
        return schema_tree;
    }
    
    /**
     * Get a description of stored procedures available in a catalog.
     * 
     * @param catalog
     *            - a catalog name; "" retrieves those without a catalog; null means drop catalog name from criteria.
     * @param schema_pattern
     *            - a schema name pattern; "" retrieves those without a schema - we ignore this parameter.
     * @param procedure_name_pattern
     *            - a procedure name pattern.
     * @return ResultSet - each row is a procedure description.
     * @exception SQLException
     *                if a database access error occurs.
     * @throws DataSourceException
     *             if posgresql version not supported.
     */
    private ResultSet getProcedures(String catalog, String schema_pattern, String procedure_name_pattern)
            throws SQLException, DataSourceException
    {
        String sql = null;
        // TODO : Why not use m_jdbc_metadata.getProcedures(catalog, schemaPattern, procedure_name_pattern)?
        if (m_jdbc_metadata.getDatabaseProductVersion().compareTo("8.1") >= 0)
        {
            sql = "SELECT NULL AS PROCEDURE_CAT, n.nspname AS PROCEDURE_SCHEM, p.proname AS PROCEDURE_NAME, NULL, NULL, NULL, "
                    + "d.description AS REMARKS, "
                    + "CASE WHEN (p.prorettype = (SELECT oid FROM pg_type WHERE typname = 'void')) THEN 1 ELSE 2 END AS PROCEDURE_TYPE, "
                    + "CAST(p.oid AS varchar) AS SPECIFIC_NAME " + "FROM pg_catalog.pg_namespace n, pg_catalog.pg_proc p "
                    + "LEFT JOIN pg_catalog.pg_description d ON (p.oid = d.objoid) "
                    + "LEFT JOIN pg_catalog.pg_class c ON (d.classoid = c.oid AND c.relname = 'pg_proc') "
                    + "LEFT JOIN pg_catalog.pg_namespace pn ON (c.relnamespace = pn.oid AND pn.nspname = 'pg_catalog') "
                    + "WHERE p.pronamespace = n.oid ";
            if (m_jdbc_metadata.getDatabaseProductVersion().compareTo("8.4") >= 0)
            {
                // Functions with variable number of arguments are not yet supported
                sql += "AND p.provariadic = 0 ";
            }
            
            if (schema_pattern != null && !"".equals(schema_pattern))
            {
                sql += " AND n.nspname LIKE '" + schema_pattern + "' ";
            }
            if (procedure_name_pattern != null)
            {
                sql += " AND p.proname LIKE '" + procedure_name_pattern + "' ";
            }
            sql += " ORDER BY PROCEDURE_SCHEM, PROCEDURE_NAME ";
        }
        else
        {
            throw new DataSourceException(DataSource.UNSUPPORTED_POSTGRESQL, null, (Object[]) null);
        }
        
        return m_connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(sql);
    }
    
    /**
     * Get a description of a catalog's stored procedure parameters and result columns.
     * 
     * @param catalog
     *            - this is ignored in org.postgresql, advise this is set to null.
     * @param schema_pattern
     *            - a schema name pattern; "" retrieves those without a schema - we ignore this parameter.
     * @param procedure_name_pattern
     *            - a procedure name pattern.
     * @param column_name_pattern
     *            - a column name pattern, this is currently ignored because postgresql does not name procedure parameters.
     * @return each row is a stored procedure parameter or column description.
     * @exception SQLException
     *                if a database-access error occurs.
     * @throws DataSourceException
     *             if there are issues with posgresql version.
     */
    private ResultSet getProcedureColumns(String catalog, String schema_pattern, String procedure_name_pattern,
            String column_name_pattern) throws SQLException, DataSourceException
    {
        Field[] f = new Field[20];
        Vector<byte[][]> v = new Vector<byte[][]>(); // The new ResultSet tuple stuff
        
        f[0] = new Field("PROCEDURE_CAT", Oid.VARCHAR);
        f[1] = new Field("PROCEDURE_SCHEM", Oid.VARCHAR);
        f[2] = new Field("PROCEDURE_NAME", Oid.VARCHAR);
        f[3] = new Field("COLUMN_NAME", Oid.VARCHAR);
        f[4] = new Field("COLUMN_TYPE", Oid.INT2);
        f[5] = new Field("DATA_TYPE", Oid.INT2);
        f[6] = new Field("TYPE_NAME", Oid.VARCHAR);
        f[7] = new Field("PRECISION", Oid.INT4);
        f[8] = new Field("LENGTH", Oid.INT4);
        f[9] = new Field("SCALE", Oid.INT2);
        f[10] = new Field("RADIX", Oid.INT2);
        f[11] = new Field("NULLABLE", Oid.INT2);
        f[12] = new Field("REMARKS", Oid.VARCHAR);
        f[13] = new Field("COLUMN_DEF", Oid.VARCHAR);
        f[14] = new Field("SQL_DATA_TYPE", Oid.INT2);
        f[15] = new Field("SQL_DATETIME_SUB", Oid.INT2);
        f[16] = new Field("CHAR_OCTET_LENGTH", Oid.INT2);
        f[17] = new Field("ORDINAL_POSITION", Oid.INT2);
        f[18] = new Field("IS_NULLABLE", Oid.VARCHAR);
        f[19] = new Field("SPECIFIC_NAME", Oid.VARCHAR);
        
        String sql = null;
        if (m_jdbc_metadata.getDatabaseProductVersion().compareTo("8.1") >= 0)
        {
            sql = "SELECT n.nspname, p.proname, p.proretset, p.prorettype, p.proargtypes, t.typtype, t.typrelid, "
                    + "p.proargnames, p.proargmodes, p.proallargtypes, CAST(p.oid AS varchar) AS SPECIFIC_NAME ";
            
            sql += "FROM pg_catalog.pg_proc p, pg_catalog.pg_namespace n, pg_catalog.pg_type t "
                    + " WHERE p.pronamespace=n.oid AND p.prorettype=t.oid ";
            if (m_jdbc_metadata.getDatabaseProductVersion().compareTo("8.4") >= 0)
            {
                // Functions with variable number of arguments are not yet supported
                sql += "AND p.provariadic = 0 ";
            }
            
            if (schema_pattern != null && !"".equals(schema_pattern))
            {
                sql += "AND n.nspname LIKE '" + schema_pattern + "' ";
            }
            if (procedure_name_pattern != null)
            {
                sql += "AND p.oid = " + procedure_name_pattern + " ";
            }
            
            sql += " ORDER BY n.nspname, p.proname ";
        }
        else
        {
            throw new DataSourceException(DataSource.UNSUPPORTED_POSTGRESQL, null, (Object[]) null);
        }
        
        ResultSet rs = m_connection.createStatement().executeQuery(sql);
        while (rs.next())
        {
            byte[] schema = rs.getBytes("nspname");
            byte[] procedure_name = rs.getBytes("proname");
            byte[] procedure_specific_name = rs.getBytes("SPECIFIC_NAME");
            boolean return_set = rs.getBoolean("proretset");
            int return_type = (int) rs.getLong("prorettype");
            String return_type_type = rs.getString("typtype");
            int return_type_relid = (int) rs.getLong("typrelid");
            
            String str_arg_types = rs.getString("proargtypes");
            StringTokenizer st = new StringTokenizer(str_arg_types);
            Vector<Long> arg_types = new Vector<Long>();
            while (st.hasMoreTokens())
            {
                arg_types.addElement(new Long(st.nextToken()));
            }
            
            String[] arg_names = null;
            Array arg_names_array = rs.getArray("proargnames");
            if (arg_names_array != null)
            {
                arg_names = (String[]) arg_names_array.getArray();
            }
            
            String[] arg_modes = null;
            Array arg_modes_array = rs.getArray("proargmodes");
            if (arg_modes_array != null)
            {
                arg_modes = (String[]) arg_modes_array.getArray();
            }
            
            int num_args = arg_types.size();
            
            Long[] all_arg_types = null;
            Array all_arg_types_array = rs.getArray("proallargtypes");
            if (all_arg_types_array != null)
            {
                // Depending on what the user has selected we'll get either long[] or Long[] back, and there's no obvious way for
                // the driver to override this for it's own usage.
                if (m_jdbc_metadata.getDatabaseProductVersion().compareTo("8.3") >= 0)
                {
                    all_arg_types = (Long[]) all_arg_types_array.getArray();
                }
                else
                {
                    long[] temp_all_arg_types = (long[]) all_arg_types_array.getArray();
                    all_arg_types = new Long[temp_all_arg_types.length];
                    for (int i = 0; i < temp_all_arg_types.length; i++)
                    {
                        all_arg_types[i] = new Long(temp_all_arg_types[i]);
                    }
                }
                num_args = all_arg_types.length;
            }
            
            // Decide if we are returning a single column result.
            if (return_type_type.equals("b") || return_type_type.equals("d")
                    || (return_type_type.equals("p") && arg_modes_array == null))
            {
                // Skip the 'void' return type
                if (return_type != m_void_type_id)
                {
                    
                    // Only name the result if it is not already named and we are returning a set
                    byte[] output_name = (return_set
                            ? (arg_modes_array != null ? null : procedure_name)
                            : "returnValue".getBytes());
                    byte[][] tuple = new byte[20][];
                    tuple[0] = null;
                    tuple[1] = schema;
                    tuple[2] = procedure_name;
                    tuple[3] = output_name;
                    // FIXME: new query processor does not support returning sets so we skip it by setting return type (tuple[4]) to unknown type
                    tuple[4] = Integer.toString((return_set
                                                        ? DatabaseMetaData.procedureColumnUnknown
                                                        : DatabaseMetaData.procedureColumnReturn)).getBytes();
                    tuple[5] = Integer.toString(((BaseConnection) m_connection).getTypeInfo().getSQLType(return_type)).getBytes();
                    tuple[6] = ((BaseConnection) m_connection).getTypeInfo().getPGType(return_type).getBytes();
                    tuple[7] = null;
                    tuple[8] = null;
                    tuple[9] = null;
                    tuple[10] = null;
                    tuple[11] = Integer.toString(java.sql.DatabaseMetaData.procedureNullableUnknown).getBytes();
                    tuple[12] = null;
                    tuple[13] = null;
                    tuple[14] = null;
                    tuple[15] = null;
                    tuple[16] = null;
                    tuple[17] = null;
                    tuple[18] = null;
                    tuple[19] = procedure_specific_name;
                    
                    v.addElement(tuple);
                }
            }
            // if we are returning a multi-column result.
            else if (return_type_type.equals("c") || (return_type_type.equals("p") && arg_modes_array != null))
            {
                // FIXME : Currently not supported because :
                // 1. Posgresql returns a java.sql.Types.Struct type for composites (not supported)
                // 2. Posgresql returns a java.sql.Types.Other type for a multi-column record (not supported)
                // When supported, then for multi-column multi-row results duplicate the first of inouts or outs columns
                // in the for directive. The duplicate column should not be named, should have
                // DatabaseMetaData.procedureColumnResult as type, and should be added before the inout/out column itself. This is
                // to ensure that the return type is
                // created before anything else.
                
                // Add unsupported column
                byte[][] tuple = new byte[20][];
                tuple[3] = "returnValue".getBytes();
                tuple[4] = Integer.toString(DatabaseMetaData.procedureColumnUnknown).getBytes();
                tuple[5] = Integer.toString(((BaseConnection) m_connection).getTypeInfo().getSQLType(return_type)).getBytes();
                v.addElement(tuple);
                break;
            }
            
            // Add a row for each in, out, inout, and/or table arguments.
            for (int i = 0; i < num_args; i++)
            {
                byte[][] tuple = new byte[20][];
                tuple[0] = null;
                tuple[1] = schema;
                tuple[2] = procedure_name;
                
                if (arg_names != null && !arg_names[i].isEmpty())
                {
                    tuple[3] = arg_names[i].getBytes();
                }
                else
                {
                    tuple[3] = ("$" + (i + 1)).getBytes();
                }
                
                int column_mode = DatabaseMetaData.procedureColumnIn;
                if (arg_modes != null && arg_modes[i].equals("o"))
                {
                    column_mode = DatabaseMetaData.procedureColumnOut;
                }
                else if (arg_modes != null && arg_modes[i].equals("b"))
                {
                    column_mode = DatabaseMetaData.procedureColumnInOut;
                }
                else if (arg_modes != null && arg_modes[i].equals("t"))
                {
                    column_mode = DatabaseMetaData.procedureColumnResult;
                }
                
                tuple[4] = Integer.toString(column_mode).getBytes();
                
                int arg_oid;
                if (all_arg_types != null)
                {
                    arg_oid = all_arg_types[i].intValue();
                }
                else
                {
                    arg_oid = ((Long) arg_types.elementAt(i)).intValue();
                }
                
                tuple[5] = Integer.toString(((BaseConnection) m_connection).getTypeInfo().getSQLType(arg_oid)).getBytes();
                tuple[6] = ((BaseConnection) m_connection).getTypeInfo().getPGType(return_type).getBytes();
                tuple[7] = null;
                tuple[8] = null;
                tuple[9] = null;
                tuple[10] = null;
                tuple[11] = Integer.toString(DatabaseMetaData.procedureNullableUnknown).getBytes();
                tuple[12] = null;
                tuple[13] = null;
                tuple[14] = null;
                tuple[15] = null;
                tuple[16] = null;
                tuple[17] = null;
                tuple[18] = null;
                tuple[19] = procedure_specific_name;
                
                v.addElement(tuple);
            }
        }
        rs.close();
        
        BaseStatement base_stmt = (BaseStatement) m_connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                                               ResultSet.CONCUR_READ_ONLY);
        
        return base_stmt.createDriverResultSet(f, v);
    }
}
