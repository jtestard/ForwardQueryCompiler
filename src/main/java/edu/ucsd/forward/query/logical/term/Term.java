/**
 * 
 */
package edu.ucsd.forward.query.logical.term;

import java.io.Serializable;
import java.util.List;

import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.exception.LocationSupport;
import edu.ucsd.forward.query.DataSourceCompliance;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.explain.ExplanationPrinter;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.ParameterUsage;
import edu.ucsd.forward.query.logical.VariableUsage;

/**
 * A term is either a function call, a query path, a parameter, or a constant. A term is not a separate operator but rather a
 * possible argument to operators. For example, the condition of the select operator is a term. Each term in a logical plan has an
 * evaluator in the physical plan that evaluates the term given only a single input binding. A term can be aliased, as for example
 * in the case of tuple construction function call and cast function call.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface Term extends VariableUsage, ParameterUsage, LocationSupport, DataSourceCompliance, ExplanationPrinter,
        Serializable
{
    /**
     * Infers the type of the term given the operator of which the term is an argument.
     * 
     * @param operators
     *            the list of operators that serve as the context of the term.
     * @return the inferred type of the term.
     * @throws QueryCompilationException
     *             if there is a query compilation error.
     */
    public Type inferType(List<Operator> operators) throws QueryCompilationException;
    
    /**
     * Gets the type of the term.
     * 
     * @return the type of the term if it is inferred; otherwise, null.
     */
    public Type getType();
    
    /**
     * Gets the alias to be used when the term appears in a Project operator and an explicit alias is not provided.
     * 
     * @return an alias.
     */
    public String getDefaultProjectAlias();
    
    /**
     * Creates a copy of the term.
     * 
     * @return a copied term.
     */
    public Term copy();
    
    /**
     * Copies the term without type. It will create problem for parameter but we worry about that later.
     * 
     * @return a copy
     */
    public Term copyWithoutType();
    
}
