/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import java.util.LinkedHashMap;
import java.util.Map;

import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;
import edu.ucsd.forward.query.ast.AttributeReference;
import edu.ucsd.forward.query.ast.QuerySpecification;
import edu.ucsd.forward.query.ast.SelectExpressionItem;
import edu.ucsd.forward.query.ast.SelectItem;
import edu.ucsd.forward.query.logical.dml.DmlOperator;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.term.RelativeVariable;

/**
 * Abstract class for implementing SQL statements.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 * @author <Vicky Papavasileiou>
 */
public abstract class AbstractJdbcStatement implements JdbcStatement
{
    private LogicalPlan                                 m_from_plan;
    
    private Map<RelativeVariable,WhereItem>             m_where_items = new LinkedHashMap<RelativeVariable,WhereItem>();
    
    protected String                                    m_dml_string;
    
    private Integer                                     m_arguments = 0;
    
    
    @Override
    public String toSql(SqlDialect sql_dialect, SqlIdentifierDictionary dictionary)
    {
         return m_dml_string;
    }
    
    public int getNumberOfArguments()
    {
        return m_arguments;
    }
  
    public void setFromPlan(LogicalPlan plan)
    {
        m_from_plan = plan;
    }
    
    public LogicalPlan getFromPlan()
    {
        return m_from_plan;
    }
    
    public boolean hasFromPlan()
    {
        if(m_from_plan != null)
            return true;
        return false;
    }
    
    protected void increseArguments()
    {
        m_arguments++;
    }
    
    public Map<RelativeVariable,WhereItem> getWhereItems()
    {
        return m_where_items;
    }
    
    public void addWhereItem(RelativeVariable var)
    {
        WhereItem item = new WhereItem(var);
        
        m_where_items.put(var,item);
    }
    
    public void setWhereRightValue(RelativeVariable left, String value)
    {
        assert(m_where_items.containsKey(left));
        m_where_items.get(left).setValue(value);
    }
    
    
    protected void setWhereRightTerms()
    {
        for(RelativeVariable var: m_where_items.keySet())
        {
            m_where_items.get(var).setValue("?");
            m_arguments++;
        }
    }    
        
    
    protected class WhereItem
    {
        private RelativeVariable    m_left;
        private String              m_right;
        private String              m_value;
        
        
        public WhereItem(RelativeVariable var)
        {
            this.m_left = var;
        }
        
        public void setRightVariable(String var)
        {
            this.m_right = var;
        }

        public void setValue(String value)
        {
            m_value = value;
        }
        
        public String toString(String table_name)
        {
            StringBuilder where = new StringBuilder();
            
            where.append(table_name + "." + m_left.getDefaultProjectAlias() + " = ");
            if(m_right != null)
            {
                where.append(m_right);
            }
            else if(m_value != null)
            {
                where.append(m_value);
            }
            return where.toString();
        }        
    }
}
