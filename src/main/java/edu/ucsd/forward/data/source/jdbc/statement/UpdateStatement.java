/**
 * 
 */
package edu.ucsd.forward.data.source.jdbc.statement;

import java.util.LinkedHashMap;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.jdbc.JdbcDataSource;
import edu.ucsd.forward.data.source.jdbc.model.SqlDialect;
import edu.ucsd.forward.data.source.jdbc.model.SqlIdentifierDictionary;
import edu.ucsd.forward.query.ast.AttributeReference;
import edu.ucsd.forward.query.ast.QueryExpression;
import edu.ucsd.forward.query.ast.QuerySpecification;
import edu.ucsd.forward.query.ast.SelectExpressionItem;
import edu.ucsd.forward.query.ast.SelectItem;
import edu.ucsd.forward.query.logical.dml.DmlOperator;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.source_wrapper.sql.PlanToAstTranslator;
import edu.ucsd.forward.util.NameGenerator;
import edu.ucsd.forward.util.NameGeneratorFactory;

/**
 * Statement for the SQL command UPDATE.
 * 
 * @author <Vicky Papavasileiou>
 * 
 */
public class UpdateStatement extends AbstractJdbcStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(UpdateStatement.class);
      
    
    private Map<RelativeVariable,SetItem>               m_assignment_items = new LinkedHashMap<RelativeVariable,SetItem>();
    
    private PlanToAstTranslator                         m_translator = new PlanToAstTranslator();
    
    private JdbcDataSource                              m_data_source;
    

    
    public UpdateStatement(JdbcDataSource data_source)
    {
        m_data_source = data_source;
    }
    
    
    public void addAssignmentItemWithPlan(RelativeVariable var, LogicalPlan plan)
    {
        SetItem set_item = new SetItem(var);
        set_item.setAsignmentPlan(plan);
        m_assignment_items.put(var,set_item);
    }
    
    public void addAssignmentItem(RelativeVariable var)
    {
        SetItem set_item = new SetItem(var);
        set_item.setAsignmentValue("?");
        increseArguments();
        m_assignment_items.put(var,set_item);
    }    
    
    public void setAssigmentItemValue(RelativeVariable var, String value)
    {
        assert(m_assignment_items.containsKey(var));
        m_assignment_items.get(var).setAsignmentValue(value);
    }
    
    public boolean hasAssignmentPlan(RelativeVariable var)
    {
        assert(m_assignment_items.containsKey(var));
        if(m_assignment_items.get(var).getAsignmentPlan() != null)
            return true;
        return false;
    }
    

    public void setDmlStatement(DmlOperator operator)
    {
        
        PlanToAstTranslator translator = new PlanToAstTranslator();
        
        // Get the target table name from the target query path
        Term target_term = operator.getTargetTerm();
        String target_table_name = target_term.getDefaultProjectAlias();
        
        StringBuilder update_query = new StringBuilder();
        
      
        update_query.append(" UPDATE " + m_data_source.getMetaData().getSchemaName() + "." + target_table_name);
        //update_query.append(" AS " + operator.getTargetScan().getAliasVariable().getName());
        
        update_query.append(" SET " );
        for (RelativeVariable var: m_assignment_items.keySet())
        {
            update_query.append(m_assignment_items.get(var).toString());
            update_query.append(" ,");
        }
        update_query.deleteCharAt(update_query.length()-1); // Delete last comma
        
         
        setWhereRightTerms();
        
        if(!getWhereItems().isEmpty())
        {
            update_query.append(" WHERE ");
            
            for(RelativeVariable left : getWhereItems().keySet())
            {
                update_query.append(getWhereItems().get(left).toString(((Update)operator).getTargetScan().getAliasVariable().getDefaultProjectAlias()));
                update_query.append(" AND ");
            }
            update_query.delete(update_query.length()-5, update_query.length()-1); // delete last AND
        }
        m_dml_string = update_query.toString();
    }    
    
 
 
    private class SetItem
    {
        private RelativeVariable    m_var;
        private LogicalPlan         m_assignment_plan;
        private String              m_assignment_value;
        
        public SetItem(RelativeVariable var)
        {
            this.m_var = var;
        }
        
        public LogicalPlan getAsignmentPlan()
        {
            return m_assignment_plan;
        }

        public void setAsignmentPlan(LogicalPlan plan)
        {
            this.m_assignment_plan = plan;
        }

        public void setAsignmentValue(String assignmentValue)
        {
            m_assignment_value = assignmentValue;
        }
        
        public String toString()
        {
            StringBuilder assignment = new StringBuilder();
            
            assignment.append(m_var.getDefaultProjectAlias() + " = ");
            if(m_assignment_plan != null)
            {
                StringBuilder sb = new StringBuilder();
                m_translator.translate(m_assignment_plan, m_data_source).toQueryString(sb, 0, m_data_source);
                
                assignment.append(" ( " + sb.toString() + ")");
            }
            else if(m_assignment_value != null)
            {
                assignment.append(m_assignment_value);
            }
            return assignment.toString();
        }
    }    
    
}
