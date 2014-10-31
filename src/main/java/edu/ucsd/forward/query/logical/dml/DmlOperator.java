/**
 * 
 */
package edu.ucsd.forward.query.logical.dml;

import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.term.Term;

/**
 * Represents a DML operator.
 * 
 * @author Yupeng
 * 
 */
public interface DmlOperator extends Operator
{
    /**
     * Returns the name of the data source targeted by the DML operator.
     * 
     * @return a data source name.
     */
    public String getTargetDataSourceName();
    
    /**
     * Returns the target term to insert.
     * 
     * @return the target term to insert.
     */
    public Term getTargetTerm();
    
    /**
     * Returns the tuple type targeted by the DML operator.
     * 
     * @return a tuple type.
     */
    public TupleType getTargetTupleType();
    
    public Scan getTargetScan();
    
}
