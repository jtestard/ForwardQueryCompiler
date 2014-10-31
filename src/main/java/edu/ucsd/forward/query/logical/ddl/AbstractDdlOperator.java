/**
 * 
 */
package edu.ucsd.forward.query.logical.ddl;

import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.AbstractOperator;
import edu.ucsd.forward.query.logical.OutputInfo;
import edu.ucsd.forward.query.logical.term.Parameter;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Variable;

/**
 * The abstract class for DDL operator.
 * 
 * @author Yupeng
 * 
 */
@SuppressWarnings("serial")
public abstract class AbstractDdlOperator extends AbstractOperator implements DdlOperator
{
    @SuppressWarnings("unused")
    private static final Logger log         = Logger.getLogger(AbstractDdlOperator.class);
    
    public static final String STATUS_ATTR = "status";
    
    /**
     * Constructs an abstract DDL operator with a given output alias.
     */
    protected AbstractDdlOperator()
    {
        super();
    }
    
    @Override
    public List<Variable> getVariablesUsed()
    {
        return Collections.emptyList();
    }
    
    @Override
    public List<Parameter> getBoundParametersUsed()
    {
        return Collections.<Parameter> emptyList();
    }
    
    @Override
    public List<Parameter> getFreeParametersUsed()
    {
        return Collections.<Parameter> emptyList();
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        // Do not have children
        assert getChildren().isEmpty();
        
        // Output info
        OutputInfo output_info = new OutputInfo();
        
        RelativeVariable new_var = new RelativeVariable(STATUS_ATTR);
        TupleType output_type = new TupleType();
        output_type.setAttribute(STATUS_ATTR, new IntegerType());
        new_var.setType(output_type);
        output_info.add(new_var, null);
        
        this.setOutputInfo(output_info);
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        switch (metadata.getDataModel())
        {
            case SQLPLUSPLUS:
                // Always compliant with SQL++ data sources
                return true;
            case RELATIONAL:
                // Never compliant with relational data sources
                return false;
            default:
                throw new AssertionError();
        }
    }
    
}
