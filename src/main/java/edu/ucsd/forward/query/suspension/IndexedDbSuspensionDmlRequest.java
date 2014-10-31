/**
 * 
 */
package edu.ucsd.forward.query.suspension;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.query.logical.dml.DmlOperator;

/**
 * 
 * The suspension request for the DML operation on indexedDB data source.
 * 
 * @author Yupeng
 * 
 */
public class IndexedDbSuspensionDmlRequest implements SuspensionRequest
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(IndexedDbSuspensionDmlRequest.class);
    
    private DmlOperator         m_operator;
    
    private List<TupleValue>    m_payload;
    
    /**
     * Constructs the indexedDB DML suspension request.
     * 
     * @param operator
     *            the DML operator that throws the exception.
     * @param payload
     *            the payload of the DML operation.
     */
    public IndexedDbSuspensionDmlRequest(DmlOperator operator, List<TupleValue> payload)
    {
        assert operator != null;
        assert payload != null;
        m_operator = operator;
        m_payload = payload;
    }
    
    /**
     * Gets the DML operator.
     * 
     * @return the DML operator that throws the exception.
     */
    public DmlOperator getDmlOperator()
    {
        return m_operator;
    }
    
    /**
     * Gets the payload of the DML operation.
     * 
     * @return the payload of the DML operation.
     */
    public List<TupleValue> getPayload()
    {
        return m_payload;
    }
}
