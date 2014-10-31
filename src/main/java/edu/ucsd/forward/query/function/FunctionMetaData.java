/**
 * 
 */
package edu.ucsd.forward.query.function;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.SqlCompliance;

/**
 * Holds the available function signatures and other static information of a function definition.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class FunctionMetaData implements SqlCompliance
{
    @SuppressWarnings("unused")
    private static final Logger     log = Logger.getLogger(FunctionMetaData.class);
    
    private String                  m_name;
    
    private Notation                m_notation;
    
    private boolean                 m_sql_compliant;
    
    private List<FunctionSignature> m_signatures;
    
    /**
     * Constructor.
     * 
     * @param name
     *            the name of the function.
     * @param notation
     *            the notation of the function.
     * @param sql_compliant
     *            whether the function is SQL compliant or not.
     */
    public FunctionMetaData(String name, Notation notation, boolean sql_compliant)
    {
        assert (name != null && !name.isEmpty());
        assert (notation != null);
        m_name = name;
        m_notation = notation;
        m_sql_compliant = sql_compliant;
        m_signatures = new ArrayList<FunctionSignature>();
    }
    
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
    public String getName()
    {
        return m_name;
    }
    
    /**
     * Gets the notation used by the function.
     * 
     * @return a function notation.
     */
    public Notation getNotation()
    {
        return m_notation;
    }
    
    @Override
    public boolean isSqlCompliant()
    {
        return m_sql_compliant;
    }
    
    /**
     * Gets the signatures of the function.
     * 
     * @return a list of function signatures.
     */
    public List<FunctionSignature> getFunctionSignatures()
    {
        return Collections.unmodifiableList(m_signatures);
    }
    
    /**
     * Adds a function signature.
     * 
     * @param signature
     *            the function signature to add.
     */
    public void addFunctionSignature(FunctionSignature signature)
    {
        assert (signature != null);
        
        m_signatures.add(signature);
    }
    
}
