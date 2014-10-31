/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.postgresql;

import java.lang.reflect.Field;

import org.testng.annotations.Test;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.source.jdbc.JdbcConnectionPool;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSource;
import edu.ucsd.forward.query.AbstractQueryTestCase;

/**
 * Tests the JDBC connection pool.
 * 
 * @author Yupeng
 * 
 */
@Test
public class TestJdbcConnectionPool extends AbstractQueryTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(TestJdbcConnectionPool.class);
    
    /**
     * Tests the configuration of pool connection.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testConfiguration() throws Exception
    {
        // Parse the test case from the XML file
        parseTestCase(this.getClass(), "TestJdbcConnectionPool-testConfiguration.xml");
        
        UnifiedApplicationState uas = getUnifiedApplicationState();
        
        JdbcDataSource jdbc_data_source = (JdbcDataSource) uas.getDataSource("jdbc");
        JdbcConnectionPool connection_pool = jdbc_data_source.getConnectionPool();
        
        Field private_pool_field = JdbcConnectionPool.class.getDeclaredField("m_pool");
        private_pool_field.setAccessible(true);
        ComboPooledDataSource combo_pool = (ComboPooledDataSource) private_pool_field.get(connection_pool);
        
        // Test the properties are correcly set.
        assertEquals(20, combo_pool.getMaxPoolSize());
        assertEquals(0, combo_pool.getUnreturnedConnectionTimeout());
        assertEquals(true, combo_pool.isDebugUnreturnedConnectionStackTraces());
        
    }
}
