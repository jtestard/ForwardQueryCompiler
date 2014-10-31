/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.model.SqlTypeConverter;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.UncheckedException;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.util.Timer;

/**
 * A data source result produced by executing a JDBC statement. This is a lightweight wrapper around a JDBC <code>ResultSet</code>,
 * and converts Java values into <code>BindingValue</code> objects.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * @author Vicky Papavasileiou
 * 
 */
public class JdbcLazyResult implements JdbcResult
{
    private static final Logger          log = Logger.getLogger(JdbcLazyResult.class);
    
    private ResultSet                    m_result_set;
    
    
    private List<Integer> m_sql_types;
    
    private Integer                     m_column_num;
    
    /**
     * Constructs a SQL result.
     * 
     * @param result_set
     *            - the JDBC result set.
     */
    public JdbcLazyResult(ResultSet result_set)
    {
        assert (result_set != null);
        m_result_set = result_set;
        m_sql_types = new ArrayList<Integer>();
        try
        {
            ResultSetMetaData metadata = m_result_set.getMetaData();
            m_column_num = metadata.getColumnCount();
            for (int i = 1; i <= m_column_num; i++)
            {
                int sql_type_code = result_set.getMetaData().getColumnType(i);
                m_sql_types.add(sql_type_code);
            }
        }
        catch (SQLException e)
        {
            throw new UncheckedException(e);
        }
    }
    
    @Override
    public void open()
    {
        // Do nothing
        Timer.reset("sum_convert");
    }
    
    @Override
    public Binding next()
    {
        try
        {
            if (m_result_set.next())
            {
                Binding binding = new Binding();
//                ResultSetMetaData metadata = m_result_set.getMetaData();
//                int column_count = metadata.getColumnCount();
                for (int i = 1; i <= m_column_num; i++)
                {
                    long time = System.currentTimeMillis();
                    // Use the type converter to convert from ResultSet to scalar values
                    Value value = SqlTypeConverter.fromSql(m_result_set, m_sql_types, i);
                    
                    // Add to output binding
                    binding.addValue(new BindingValue(value, true));
                    Timer.inc("sum_convert", System.currentTimeMillis() - time);
                }
                
                return binding;
            }
            else
            {               
                log.debug("Result conversion time and create binding: " + Timer.get("sum_convert") + Timer.MS);
                return null;
            }
        }
        catch (SQLException e)
        {
            throw new UncheckedException(e);
        }
    }
    
    @Override
    public void close()
    {
        try
        {
//            log.debug("Result conversion time and create binding: " + Timer.get("sum_convert") + Timer.MS);
            Timer.reset("sum_convert");
            m_result_set.close();            
        }
        catch (SQLException e)
        {
            throw new UncheckedException(e);
        }
    }
    
}
