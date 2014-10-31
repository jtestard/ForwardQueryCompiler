/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSource;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.logical.dml.DmlOperator;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;

/**
 * Statement for the SQL command DELETE.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author <Vicky Papavasileiou>
 */
public class DeleteStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DeleteStatement.class);

    private JdbcDataSource                              m_data_source;
    
    
    public DeleteStatement(JdbcDataSource data_source)
    {
        m_data_source = data_source;
    }
    
    public void setDmlStatement(DmlOperator operator)
    {
        Delete delete = (Delete)operator;
        Term target_term = operator.getTargetTerm();
        String target_table_name = target_term.getDefaultProjectAlias();
        
        StringBuilder delete_query = new StringBuilder();

        delete_query.append(" DELETE FROM " + m_data_source.getMetaData().getSchemaName() + "." + target_table_name);
        delete_query.append(" AS " + delete.getTargetScan().getAliasVariable().getName() );
        
        setWhereRightTerms();
        
        if(!getWhereItems().isEmpty())
        {
            delete_query.append(" WHERE ");
            
            for(RelativeVariable left : getWhereItems().keySet())
            {
                delete_query.append(getWhereItems().get(left).toString(operator.getTargetScan().getAliasVariable().getName()));
                delete_query.append(" AND ");
            }
            delete_query.delete(delete_query.length()-5, delete_query.length()-1); // delete last AND
        }
        m_dml_string = delete_query.toString();
    }    
    
}
