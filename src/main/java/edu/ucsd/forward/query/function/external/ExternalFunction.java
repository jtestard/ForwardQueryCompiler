/**
 * 
 */
package edu.ucsd.forward.query.function.external;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.jdbc.JdbcEagerResult;
import edu.ucsd.forward.data.source.jdbc.JdbcExecution;
import edu.ucsd.forward.data.source.jdbc.JdbcTransaction;
import edu.ucsd.forward.data.source.jdbc.statement.GenericStatement;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.NullType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.TupleType.AttributeEntry;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessor.DataSourceAccess;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.function.AbstractFunction;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.term.Constant;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.TermEvaluator;
import edu.ucsd.forward.query.source_wrapper.sql.PlanToAstTranslator;

/**
 * Represents external functions, such as stored procedures defined in a linked data source. External function are always databas
 * compatible.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class ExternalFunction extends AbstractFunction implements ExternalFunctionEvaluator
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ExternalFunction.class);
    
    /**
     * Constructor.
     * 
     * @param target_data_source
     *            the target data source to evaluate the function.
     * @param name
     *            the name of the function.
     */
    public ExternalFunction(String target_data_source, String name)
    {
        super(name);
        assert (target_data_source != null && !target_data_source.isEmpty());
        m_data_source = target_data_source;
    }
    
    /**
     * Gets the target data source to evaluate the function.
     * 
     * @return the target data source to evaluate the function.
     */
    public String getTargetDataSource()
    {
        return m_data_source;
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return true;
    }
    
    @Override
    public BindingValue evaluate(ExternalFunctionCall call, Binding input) throws QueryExecutionException
    {
        // Construct a logical plan to send to the external data source
        LogicalPlan external_plan = null;
        
        // Construct the external function call
        List<Term> arg_values = new ArrayList<Term>();
        for (Term arg : call.getArguments())
        {
            BindingValue value = TermEvaluator.evaluate(arg, input);
            assert (value.getSqlValue() instanceof ScalarValue);
            arg_values.add(new Constant((ScalarValue) value.getSqlValue()));
        }
        ExternalFunctionCall external_call = new ExternalFunctionCall(call.getFunction(), arg_values);
        
        Type return_type = call.getType();
        try
        {
            // For external function returning a scalar, construct the following plan:
            // SELECT external_function(...);
            if (return_type instanceof ScalarType)
            {
                Project project = new Project();
                project.addProjectionItem(external_call, this.getName(), true);
                project.addChild(new Ground());
                external_plan = new LogicalPlan(project);
            }
            // For void external function, construct the following AST:
            // SELECT NULL FROM external_function(...);
            else if (return_type instanceof NullType)
            {
                throw new UnsupportedOperationException();
            }
            // For external function returning a tuple or a collection, construct the following AST:
            // SELECT * FROM external_function(...);
            else if (return_type instanceof CollectionType || return_type instanceof TupleType)
            {
                TupleType tuple_type = (return_type instanceof TupleType)
                        ? (TupleType) return_type
                        : ((CollectionType) return_type).getTupleType();
                Project project = new Project();
                for (AttributeEntry entry : tuple_type)
                {
                    project.addProjectionItem(new QueryPath(new RelativeVariable(this.getName()),
                                                            Collections.singletonList(entry.getName())), entry.getName(), true);
                }
                Scan scan = new Scan(this.getName(), external_call);
                scan.addChild(new Ground());
                project.addChild(scan);
                external_plan = new LogicalPlan(project);
            }
            else
            {
                throw new UnsupportedOperationException();
            }
            external_plan.updateOutputInfoDeep();
        }
        catch (QueryCompilationException e)
        {
            // This should never happen
            throw new AssertionError();
        }
        
        // Translate the logical plan
        PlanToAstTranslator translator = new PlanToAstTranslator();
        StringBuilder sb = new StringBuilder();
        DataSourceAccess target_data_source_access = QueryProcessorFactory.getInstance().getDataSourceAccess(m_data_source);
        DataSource target_data_source = target_data_source_access.getDataSource();
        translator.translate(external_plan, target_data_source).toQueryString(sb, 0, target_data_source);
        String query_string = sb.toString();
        
        // Build the query statement
        GenericStatement statement = new GenericStatement(query_string);
        
        // Execute the query statement
        JdbcEagerResult result = JdbcExecution.run((JdbcTransaction) target_data_source_access.getTransaction(), statement);
        
        // For external function returning a scalar, return the only binding value of the only binding
        if (return_type instanceof ScalarType)
        {
            BindingValue return_value = null;
            
            result.open();
            Binding binding = result.next();
            if (binding != null)
            {
                assert (binding.size() == 1);
                return_value = binding.getValue(0);
                binding = result.next();
                assert (binding == null);
            }
            else
            {
                return_value = new BindingValue(new NullValue(), true);
            }
            result.close();
            
            return return_value;
        }
        // For void external function, construct the following AST:
        // SELECT NULL FROM external_function(...);
        else if (return_type instanceof NullType)
        {
            throw new UnsupportedOperationException();
        }
        // For external function returning a tuple, return the only binding
        else if (return_type instanceof TupleType)
        {
            Value return_value = null;
            
            result.open();
            Binding binding = result.next();
            if (binding != null)
            {
                TupleValue tuple_value = new TupleValue();
                int index = 0;
                for (AttributeEntry entry : (TupleType) return_type)
                {
                    tuple_value.setAttribute(entry.getName(), binding.getValue(index++).getValue());
                }
                return_value = tuple_value;
                binding = result.next();
                assert (binding == null);
            }
            else
            {
                return_value = new NullValue();
            }
            result.close();
            
            return new BindingValue(return_value, true);
        }
        // For external function returning a collection, return all bindings
        else if (return_type instanceof CollectionType)
        {
            CollectionValue return_value = new CollectionValue();
            
            result.open();
            Binding binding = result.next();
            while (binding != null)
            {
                TupleValue tuple_value = new TupleValue();
                int index = 0;
                for (AttributeEntry entry : ((CollectionType) return_type).getTupleType())
                {
                    tuple_value.setAttribute(entry.getName(), binding.getValue(index++).getValue());
                }
                return_value.add(tuple_value);
                binding = result.next();
            }
            result.close();
            
            return new BindingValue(return_value, false);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
    
}
