/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.model;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.DateType;
import edu.ucsd.forward.data.type.DecimalType;
import edu.ucsd.forward.data.type.DoubleType;
import edu.ucsd.forward.data.type.FloatType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.LongType;
import edu.ucsd.forward.data.type.NullType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.TimestampType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.XhtmlType;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.DateValue;
import edu.ucsd.forward.data.value.DecimalValue;
import edu.ucsd.forward.data.value.DoubleValue;
import edu.ucsd.forward.data.value.FloatValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.LongValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.TimestampValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.UncheckedException;

/**
 * Converter between data types and SQL types.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Vicky Papavasileiou
 */
public final class SqlTypeConverter
{
    @SuppressWarnings("unused")
    private static final Logger                                    log = Logger.getLogger(SqlTypeConverter.class);
    
    private static final Map<Class<? extends ScalarType>, Integer> TYPE_TO_SQL_TYPE_CODE;
    
    static
    {
        Map<Class<? extends ScalarType>, Integer> m = new HashMap<Class<? extends ScalarType>, Integer>();
        m.put(IntegerType.class, Types.INTEGER);
        m.put(LongType.class, Types.BIGINT);
        m.put(FloatType.class, Types.FLOAT);
        m.put(DoubleType.class, Types.DOUBLE);
        m.put(DecimalType.class, Types.DECIMAL);
        m.put(DateType.class, Types.DATE);
        m.put(TimestampType.class, Types.TIMESTAMP);
        // Use bit instead of boolean due to "bug" in JDBC for postgres:
        // http://archives.postgresql.org/pgsql-jdbc/2004-04/msg00107.php
        m.put(BooleanType.class, Types.BIT);
        m.put(StringType.class, Types.VARCHAR);
        m.put(XhtmlType.class, Types.VARCHAR);
        TYPE_TO_SQL_TYPE_CODE = Collections.unmodifiableMap(m);
    }
    
    private static final Map<Integer, Class<? extends ScalarType>> SQL_TYPE_TO_TYPE_CODE;
    
    static
    {
        Map<Integer, Class<? extends ScalarType>> m = new HashMap<Integer, Class<? extends ScalarType>>();
        
        m.put(Types.SMALLINT, IntegerType.class);
        m.put(Types.INTEGER, IntegerType.class);
        m.put(Types.BIGINT, LongType.class);
        m.put(Types.REAL, FloatType.class);
        m.put(Types.DOUBLE, DoubleType.class);
        m.put(Types.DECIMAL, DecimalType.class);
        m.put(Types.NUMERIC, DecimalType.class);
        m.put(Types.DATE, DateType.class);
        m.put(Types.TIMESTAMP, TimestampType.class);
        // Use bit instead of boolean due to "bug" in JDBC for postgres:
        // http://archives.postgresql.org/pgsql-jdbc/2004-04/msg00107.php
        m.put(Types.BIT, BooleanType.class);
        m.put(Types.CHAR, StringType.class);
        m.put(Types.VARCHAR, StringType.class);
        m.put(Types.LONGVARCHAR, StringType.class);
        SQL_TYPE_TO_TYPE_CODE = Collections.unmodifiableMap(m);
    }
    
    private static final Map<Class<? extends ScalarType>, String>  TYPE_TO_SQL_TYPE_NAME;
    
    static
    {
        Map<Class<? extends ScalarType>, String> m = new HashMap<Class<? extends ScalarType>, String>();
        m.put(IntegerType.class, "int4");
        m.put(LongType.class, "bigint");
        m.put(FloatType.class, "real");
        m.put(DoubleType.class, "double precision");
        m.put(DecimalType.class, "decimal");
        m.put(DateType.class, "date");
        m.put(TimestampType.class, "timestamp");
        m.put(BooleanType.class, "boolean");
        m.put(StringType.class, "varchar");
        m.put(XhtmlType.class, "varchar");
        TYPE_TO_SQL_TYPE_NAME = Collections.unmodifiableMap(m);
    }
    
    private static final Map<Integer, String>                      SQL_TYPE_CODE_TO_TYPE_NAME;
    
    static
    {
        Map<Integer, String> m = new HashMap<Integer, String>();
        m.put(2003, "ARRAY");
        m.put(-5, "BIGINT");
        m.put(-2, "BINARY");
        m.put(-7, "BIT");
        m.put(2004, "BLOB");
        m.put(16, "BOOLEAN");
        m.put(1, "CHAR");
        m.put(2005, "CLOB");
        m.put(70, "DATALINK");
        m.put(91, "DATE");
        m.put(3, "DECIMAL");
        m.put(2001, "DISTINCT");
        m.put(8, "DOUBLE");
        m.put(6, "FLOAT");
        m.put(4, "INTEGER");
        m.put(2000, "JAVA_OBJECT");
        m.put(-16, "LONGNVARCHAR");
        m.put(-4, "LONGVARBINARY");
        m.put(-1, "LONGVARCHAR");
        m.put(-15, "NCHAR");
        m.put(2011, "NCLOB");
        m.put(0, "NULL");
        m.put(2, "NUMERIC");
        m.put(-9, "NVARCHAR");
        m.put(1111, "OTHER");
        m.put(7, "REAL");
        m.put(2006, "REF");
        m.put(-8, "ROWID");
        m.put(5, "SMALLINT");
        m.put(2009, "SQLXML");
        m.put(2002, "STRUCT");
        m.put(92, "TIME");
        m.put(93, "TIMESTAMP");
        m.put(-6, "TINYINT");
        m.put(-3, "VARBINARY");
        m.put(12, "VARCHAR");
        SQL_TYPE_CODE_TO_TYPE_NAME = Collections.unmodifiableMap(m);
    }
    
    /**
     * Private constructor.
     * 
     */
    private SqlTypeConverter()
    {
        
    }
    
    /**
     * Returns the corresponding SQL type code (as defined by JDBC).
     * 
     * This will be the exact type code as returned by <code>java.sql.ResultSetMetaData.getColumnType()</code>.
     * 
     * @param type_class
     *            - the type class.
     * @return the corresponding SQL type code.
     */
    public static int getSqlTypeCode(Class<? extends ScalarType> type_class)
    {
        return TYPE_TO_SQL_TYPE_CODE.get(type_class);
    }
    
    /**
     * Returns the corresponding SQL type name (as used by the database).
     * 
     * This will be the exact string as returned by <code>java.sql.ResultSetMetaData.getColumnTypeName()</code>, including
     * case-sensitivity.
     * 
     * @param type_class
     *            - the type class.
     * @return the corresponding SQL type name.
     */
    public static String getSqlTypeName(Class<? extends ScalarType> type_class)
    {
        return TYPE_TO_SQL_TYPE_NAME.get(type_class);
    }
    
    /**
     * Returns the corresponding type class.
     * 
     * @param sql_type_code
     *            - the SQL type code.
     * @return the corresponding type class.
     */
    public static Class<? extends ScalarType> getType(int sql_type_code)
    {
        return SQL_TYPE_TO_TYPE_CODE.get(sql_type_code);
    }
    
    /**
     * Returns the corresponding type name.
     * 
     * @param sql_type_code
     *            - the SQL type code.
     * @return the corresponding type name.
     */
    public static String getSqlTypeNameFromTypeCode(int sql_type_code)
    {
        return SQL_TYPE_CODE_TO_TYPE_NAME.get(sql_type_code);
    }
    
    /**
     * Produces a value corresponding to the type by reading from the current row of a JDBC result set. If the JDBC result set
     * contains a SQL NULL, this method will return a NullValue.
     * 
     * Concrete classes should provide a more specific class for the value.
     * 
     * @param result_set
     *            - the JDBC result set.
     * @param index
     *            - the 1-based position index to read from.
     * @return the value.
     */
    public static Value fromSql(ResultSet result_set, List<Integer> list, int index)
    {
        try
        {
            Value value;
 
            int sql_type_code = list.get(index - 1);
            Class<? extends Type> type_class = null;
            
            Object object = result_set.getObject(index);
            
            if (result_set.wasNull())
            {
                value = new NullValue(SQL_TYPE_TO_TYPE_CODE.get(sql_type_code));
                return value;
            }
            
            switch (sql_type_code)
            {
                case Types.SMALLINT:
                    value = new IntegerValue((Integer) object);
                    break;
                case Types.BIGINT:
                    value = new LongValue((Long) object);
                    break;
                case Types.INTEGER:
                    value = new IntegerValue((Integer) object);
                    break;
                case Types.REAL:
                    value = new FloatValue((Float) object);
                    break;
                case Types.DOUBLE:
                    value = new DoubleValue((Double) object);
                    break;
                case Types.DECIMAL:
                    value = new DecimalValue((BigDecimal) object);
                    break;
                case Types.NUMERIC:
                    value = new DecimalValue((BigDecimal) object);
                    break;
                case Types.DATE:
                    value = new DateValue((Date) object);
                    break;
                case Types.TIMESTAMP:
                    value = new TimestampValue((Timestamp) object);
                    break;
                case Types.BIT:
                    value = new BooleanValue((Boolean) object);
                    break;
                case Types.CHAR:
                    value = new StringValue((String) object);
                    break;
                case Types.VARCHAR:
                    value = new StringValue((String) object);
                    break;
                case Types.LONGVARCHAR:
                    value = new StringValue((String) object);
                    break;
                case Types.NULL:
                    value = new NullValue(NullType.class);
                    break;
                default:
                    type_class = SQL_TYPE_TO_TYPE_CODE.get(sql_type_code);
                    assert (type_class == StringType.class) : "Unknown type! Expect StringType, found " + type_class;
                    value = new StringValue((String) object);
            }
            
            return value;
            
        }
        catch (SQLException e)
        {
            throw new UncheckedException(e);
        }
    }
    
    /**
     * Sets a value corresponding to the type into a parameter of a JDBC prepared statement.
     * 
     * @param prepared_statement
     *            - the prepared statement.
     * @param index
     *            - the 1-based position index of the parameter.
     * @param value
     *            - the value.
     */
    public static void toSql(PreparedStatement prepared_statement, int index, Value value)
    {
        try
        {
            if (value instanceof NullValue)
            {
                if (value.getTypeClass() == NullType.class)
                {
                    prepared_statement.setNull(index, Types.NULL);
                }
                else
                {
                    @SuppressWarnings("unchecked")
                    Class<? extends ScalarType> type_class = (Class<? extends ScalarType>) value.getTypeClass();
                    prepared_statement.setNull(index, getSqlTypeCode(type_class));
                }
                
            }
            else
            {
                assert (value instanceof ScalarValue);
                prepared_statement.setObject(index, ((ScalarValue) value).getObject(),
                                             getSqlTypeCode(((ScalarValue) value).getTypeClass()));
            }
        }
        catch (SQLException e)
        {
            throw new UncheckedException(e);
        }
    }
}
