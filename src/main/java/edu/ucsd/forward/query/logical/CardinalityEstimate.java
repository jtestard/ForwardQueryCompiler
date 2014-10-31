package edu.ucsd.forward.query.logical;

import java.io.Serializable;

import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.exception.ExceptionMessages;

/**
 * An interface for estimating the cardinality of an operator's output.
 * 
 * @author Michalis Petropoulos
 * @author Yupeng
 */
public interface CardinalityEstimate extends Serializable
{
    
    /**
     * An enumeration of cardinality estimates for the operator's output.
     */
    public enum Size
    {
        UNKNOWN,

        LARGE,

        SMALL,

        ONE,

        ZERO;
        
        /**
         * Returns the cardinality estimate constant with the specified name.
         * 
         * @param name
         *            the name of the constant to return.
         * @return the cardinality estimate constant with the specified name.
         * @throws DataSourceException
         *             if there is no cardinality estimate constant with the specified name.
         */
        public static Size constantOf(String name) throws DataSourceException
        {
            try
            {
                return Size.valueOf(name);
            }
            catch (IllegalArgumentException ex)
            {
                throw new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_CARDINALITY_ESTIMATE, name);
            }
        }
    }
    
    /**
     * Returns the cardinality estimate.
     * 
     * @return the cardinality estimate.
     */
    public Size getCardinalityEstimate();
    
    /**
     * Sets the cardinality estimate.
     * 
     * @param size
     *            the cardinality estimate.
     */
    public void setCardinalityEstimate(Size size);
    
}
