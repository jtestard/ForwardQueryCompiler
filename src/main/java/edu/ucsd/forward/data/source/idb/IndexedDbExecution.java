/**
 * 
 */
package edu.ucsd.forward.data.source.idb;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.logical.dml.DmlOperator;
import edu.ucsd.forward.query.logical.dml.Insert;
import edu.ucsd.forward.query.logical.dml.Update;
import edu.ucsd.forward.query.logical.term.AbsoluteVariable;
import edu.ucsd.forward.query.logical.term.QueryPath;
import edu.ucsd.forward.query.logical.term.RelativeVariable;
import edu.ucsd.forward.query.logical.term.Variable;
import edu.ucsd.forward.query.physical.Binding;
import edu.ucsd.forward.query.physical.BindingValue;
import edu.ucsd.forward.query.physical.IndexScanImpl;
import edu.ucsd.forward.query.physical.OperatorImpl;
import edu.ucsd.forward.query.physical.QueryPathEvaluator;
import edu.ucsd.forward.query.physical.ScanImpl;

/**
 * An execution of an IndexedDB plan.
 * 
 * @author Yupeng
 * 
 */
public final class IndexedDbExecution
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(IndexedDbExecution.class);
    
    /**
     * Hidden constructor.
     */
    private IndexedDbExecution()
    {
        
    }
    
    /**
     * Executes the DML operator and applies the payload.
     * 
     * @param operator
     *            the DML operator.
     * @param payload
     *            the payload of the DML operation.
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    public static void executeUpdate(DmlOperator operator, List<TupleValue> payload) throws QueryExecutionException
    {
        assert operator != null;
        assert payload != null;
        // Get the target data tree
        String db_name = operator.getTargetDataSourceName();
        String os_name = ((Variable) operator.getTargetTerm()).getDefaultProjectAlias();
        DataSource source = QueryProcessorFactory.getInstance().getDataSourceAccess(db_name).getDataSource();
        DataTree data_tree = source.getDataObject(os_name);
        CollectionValue collection = ((CollectionValue) data_tree.getRootValue());
        if (operator instanceof Insert)
        {
            for (TupleValue tuple : payload)
            {
                collection.add(tuple);
            }
        }
        else if (operator instanceof Delete)
        {
            for (TupleValue tuple : payload)
            {
                collection.remove(tuple);
            }
        }
        else
        {
            assert operator instanceof Update;
            for (TupleValue tuple : payload)
            {
                DataPath path = new DataPath(tuple.getParent(), tuple);
                TupleValue tuple_to_remove = (TupleValue) path.find(collection);
                collection.remove(tuple_to_remove);
                collection.add(tuple);
            }
        }
    }
    
    /**
     * Executes the plan.
     * 
     * @param result
     *            the IndexedDB result that holds the plan and the buffer.
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    public static void execute(IndexedDbResult result) throws QueryExecutionException
    {
        assert result != null;
        OperatorImpl root_op = result.getPhysicalPlan().getRootOperatorImpl();
        if (root_op instanceof ScanImpl) executeScanImpl((ScanImpl) root_op, result);
        else if (root_op instanceof IndexScanImpl) executeIndexScanImpl((IndexScanImpl) root_op, result);
        else throw new UnsupportedOperationException();
    }
    
    /**
     * Executes the scan implementation on indexedDB data source.
     * 
     * @param scan
     *            the scan implementation
     * @param result
     *            the IndexedDB result that holds the plan and the buffer.
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    private static void executeScanImpl(ScanImpl scan, IndexedDbResult result) throws QueryExecutionException
    {
        // Loads the data into the buffer
        AbsoluteVariable variable = (AbsoluteVariable) scan.getOperator().getTerm();
        String db_name = variable.getDataSourceName();
        String os_name = variable.getSchemaObjectName();
        DataSource source = QueryProcessorFactory.getInstance().getDataSourceAccess(db_name).getDataSource();
        // Gets the data from the cache.
        CollectionValue collection = (CollectionValue) source.getDataObject(os_name).getRootValue();
        
        // Keep only the attributes specified by the scan operator
        List<Binding> bindings = new ArrayList<Binding>();
        
        List<QueryPath> retain_attributes = new ArrayList<QueryPath>();
//        for (RelativeVariable var : ((Scan) scan.getOperator()).getNavigateVariables().keySet())
//        {
//            retain_attributes.add((QueryPath) ((Scan) scan.getOperator()).getNavigateVariables().get(var));
//        }
//        
        for (TupleValue next_value : collection.getTuples())
        {
            Binding binding = new Binding();
            for (QueryPath qp : retain_attributes)
            {
                Value navigated_value = QueryPathEvaluator.evaluateFromValue(qp, next_value);
                binding.addValue(new BindingValue(navigated_value, false));
            }
            bindings.add(binding);
        }
        
        result.setBuffer(bindings);
        result.closePopulation();
    }
    
    /**
     * Executes the index scan implementation on indexedDB data source.
     * 
     * @param index_scan
     *            the index scan implementation
     * @param result
     *            the IndexedDB result that holds the plan and the buffer.
     * @throws QueryExecutionException
     *             if an exception is thrown during query execution.
     */
    private static void executeIndexScanImpl(IndexScanImpl index_scan, IndexedDbResult result) throws QueryExecutionException
    {
        // // Loads the data into the buffer
        // AbsoluteVariable variable = (AbsoluteVariable) index_scan.getOperator().getTerm();
        // String db_name = variable.getDataSourceName();
        // String os_name = variable.getSchemaObjectName();
        // DataSource source = QueryProcessorFactory.getInstance().getDataSourceAccess(db_name).getDataSource();
        //
        // List<KeyRange> ranges = KeyRangeEvaluator.evaluate(new Binding(), index_scan.getOperator().getKeyRangeSpecs());
        // // Gets the data from the index.
        // List<TupleValue> list = source.getFromIndex(os_name, SchemaPath.root(), index_scan.getOperator().getIndexName(), ranges);
        // result.setBuffer(list);
        // result.closePopulation();
        throw new UnsupportedOperationException();
    }
}
