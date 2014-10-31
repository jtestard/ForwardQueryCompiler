/**
 * 
 */
package edu.ucsd.forward.query.function;

import java.util.List;

import edu.ucsd.forward.query.SqlCompliance;

/**
 * The function definition interface. During query compilation, a function definition contributes to the type inference by
 * specifying the required arguments type and the return type of the function.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
public interface Function extends SqlCompliance, FunctionEvaluator
{
    /**
     * Function notation options.
     * 
     * @author Michalis Petropoulos
     * 
     */
    public enum Notation
    {
        PREFIX, INFIX, POSTFIX, OTHER
    }
    
    /**
     * Gets the name of the function.
     * 
     * @return the name of the function.
     */
    public String getName();
    
    /**
     * Gets the notation used by the function.
     * 
     * @return a function notation.
     */
    public Notation getNotation();
    
    /**
     * Gets the signatures of the function.
     * 
     * @return a list of function signatures.
     */
    public List<FunctionSignature> getFunctionSignatures();
    
    
    /**
     * @return the data_source
     */
    public String getDataSourceName();
    
    /**
     * @param data_source the data_source to set
     */
    public void setDataSource(String data_source);
    
    // /**
    // * Gets the metadata of the function.
    // *
    // * @return the metadata of the function.
    // */
    // public FunctionMetaData getMetaData();
    
}
