/**
 * 
 */
package edu.ucsd.forward.query.function.aggregate;

import edu.ucsd.forward.query.function.Function;

/**
 * Represents an aggregate function definition that has a scalar return type.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface AggregateFunction extends Function, AggregateFunctionEvaluator
{
}
