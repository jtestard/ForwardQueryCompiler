/**
 * 
 */
package edu.ucsd.forward.query.logical;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents the semi-join operator. A semi-join returns a binding from the left child operator implementation when at least one
 * matching binding is found in the right child operator implementation. The difference between a semi-join and a conventional join
 * is that bindings in the left child will be returned at most once.
 * 
 * @author Yupeng
 * 
 */
@SuppressWarnings("serial")
public class SemiJoin extends InnerJoin
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(SemiJoin.class);
    
    /**
     * Initializes an instance of the operator.
     */
    public SemiJoin()
    {
        super();
    }
    
    @Override
    public void updateOutputInfo() throws QueryCompilationException
    {
        for (Term condition : this.getConditions())
            assert (condition.inferType(this.getChildren()) instanceof BooleanType);
        
        // Output info
        OutputInfo output_info = new OutputInfo();
        
        // Get the left info
        OutputInfo left_info = this.getChildren().get(0).getOutputInfo();
        
        // Copy the input variables and keys from the left child
        for (RelativeVariable var : left_info.getVariables())
        {
            output_info.add(var, var);
        }
        output_info.setKeyTerms(left_info.getKeyTerms());
        
        // Set the actual output info
        this.setOutputInfo(output_info);
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        switch (metadata.getDataModel())
        {
            case SQLPLUSPLUS:
                return super.isDataSourceCompliant(metadata);
            case RELATIONAL:
                // Never compliant with relational data sources
                return false;
            default:
                throw new AssertionError();
        }
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitSemiJoin(this);
    }
    
    @Override
    public Operator copy()
    {
        SemiJoin copy = new SemiJoin();
        super.copy(copy);
        
        for (Term term : this.getConditions())
            copy.addCondition(term.copy());
        
        return copy;
    }
    
    @Override
    public SemiJoin copyWithoutType()
    {
        SemiJoin copy = new SemiJoin();
        
        for (Term term : this.getConditions())
            copy.addCondition(term.copyWithoutType());
        
        return copy;
    }
}
