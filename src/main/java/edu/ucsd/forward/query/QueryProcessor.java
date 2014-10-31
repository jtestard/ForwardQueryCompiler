package edu.ucsd.forward.query;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.source.DataSourceTransaction;
import edu.ucsd.forward.data.source.DataSourceTransaction.TransactionState;
import edu.ucsd.forward.data.source.UnifiedApplicationState;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.exception.UncheckedException;
import edu.ucsd.forward.fpl.FplParsingException;
import edu.ucsd.forward.fpl.ast.ActionInvocation;
import edu.ucsd.forward.fpl.ast.Definition;
import edu.ucsd.forward.fpl.ast.VariableDeclaration;
import edu.ucsd.forward.query.ast.AstTree;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.visitors.LogicalPlanBuilder;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlan;
import edu.ucsd.forward.query.logical.plan.LogicalPlanDistributor;
import edu.ucsd.forward.query.logical.plan.LogicalPlanUtil;
import edu.ucsd.forward.query.logical.rewrite.PushConditionsDownRewriter;
import edu.ucsd.forward.query.logical.visitors.JoinOrderRewriter;
import edu.ucsd.forward.query.logical.visitors.PhysicalPlanBuilder;
import edu.ucsd.forward.query.parser.ParseException;
import edu.ucsd.forward.query.parser.QueryParser;
import edu.ucsd.forward.query.physical.CopyContext;
import edu.ucsd.forward.query.physical.ParameterInstantiations;
import edu.ucsd.forward.query.physical.SendPlanImpl;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;
import edu.ucsd.forward.query.source_wrapper.SourceWrapper;
import edu.ucsd.forward.query.source_wrapper.in_memory.InMemorySourceWrapper;
import edu.ucsd.forward.query.source_wrapper.sql.SqlSourceWrapper;
import edu.ucsd.forward.util.Timer;

/**
 * Represents the query processor.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class QueryProcessor
{
    private static final Logger           log            = Logger.getLogger(QueryProcessor.class);
    
    /**
     * The query parser.
     */
    private static QueryParser            s_query_parser = null;
    
    /**
     * The unified application state.
     */
    private UnifiedApplicationState       m_uas          = null;
    
    /**
     * The set of data sources accessed during query execution.
     */
    private Map<String, DataSourceAccess> m_accessed_data_sources;
    
    /**
     * The parameter instantiations that the query processor will use.
     */
    private ParameterInstantiations       m_param_insts;
    
    /**
     * Hidden constructor. Switched from protected to public for testing purposes
     */
    public QueryProcessor()
    {
        m_accessed_data_sources = new HashMap<String, DataSourceAccess>();
        m_param_insts = new ParameterInstantiations();
    }
    
    /**
     * Gets the unified application state.
     * 
     * @return the unified application state.
     */
    public UnifiedApplicationState getUnifiedApplicationState()
    {
        return m_uas;
    }
    
    /**
     * Sets the unified application state.
     * 
     * @param uas
     *            the unified application state.
     */
    public void setUnifiedApplicationState(UnifiedApplicationState uas)
    {
        assert (uas != null);
        if (uas.getState() == UnifiedApplicationState.State.CLOSED)
        {
            m_uas = uas;
        }
        
        assert (uas.getState() == UnifiedApplicationState.State.OPEN);
        
        m_uas = uas;
    }
    
    /**
     * Gets the parser.
     * 
     * @param stmt
     *            the statement to parse.
     * @return the parser.
     */
    private QueryParser getParser(String stmt)
    {
        if (s_query_parser == null)
        {
            s_query_parser = new QueryParser(new StringReader(stmt));
        }
        return s_query_parser;
    }
    
    /**
     * Parses a variable declaration.
     * 
     * @param stmt
     *            The statement to parse.
     * @param location
     *            the location of the statement.
     * @return the variable declaration.
     * @throws ParseException .
     * @throws FplParsingException .
     */
    public List<VariableDeclaration> parseDefineVariableDeclaration(String stmt, Location location)
            throws FplParsingException, ParseException
    {
        if (s_query_parser == null)
        {
            getParser(stmt);
        }
        else
        {
            QueryParser.ReInit(new StringReader(stmt));
        }
        // We subtract 1 because the parser starts at line 1 instead of line 0
        String path = location != null ? location.getPath() : null;
        int offset_line = location != null ? location.getStartLine() - 1 : 0;
        int offset_column = location != null ? location.getStartColumn() - 1 : 0;
        QueryParser.setStaticLocationInfo(path, offset_line, offset_column);
        
        return QueryParser.define_variable_declarations();
    }
    
    /**
     * Parses an init expression.
     * 
     * @param stmt
     *            The statement to parse.
     * @param location
     *            the location of the statement.
     * @return the value expression
     * @throws ParseException .
     * @throws FplParsingException .
     */
    public ValueExpression parseInitExpression(String stmt, Location location) throws FplParsingException, ParseException
    {
        if (s_query_parser == null)
        {
            getParser(stmt);
        }
        else
        {
            QueryParser.ReInit(new StringReader(stmt));
        }
        // We subtract 1 because the parser starts at line 1 instead of line 0
        String path = location != null ? location.getPath() : null;
        int offset_line = location != null ? location.getStartLine() - 1 : 0;
        int offset_column = location != null ? location.getStartColumn() - 1 : 0;
        QueryParser.setStaticLocationInfo(path, offset_line, offset_column);
        
        return QueryParser.expression();
    }
    
    /**
     * Parses an input statement and returns a list of AST trees.
     * 
     * @param stmt
     *            the statement to parse.
     * @param location
     *            the location of the statement.
     * @return a list of AST trees.
     * @throws QueryParsingException
     *             if an error occurs during statement parsing.
     */
    public List<AstTree> parseQuery(String stmt, Location location) throws QueryParsingException
    {
        return this.parseQuery(stmt, (location != null) ? location.getPath() : null, 0, 0);
    }
    
    /**
     * Parses an input statement and returns a list of AST trees.
     * 
     * @param stmt
     *            the statement to parse.
     * @param path
     *            path
     * @param offset_line
     *            offset_line
     * @param offset_column
     *            offset_column
     * @return a list of AST trees.
     * @throws QueryParsingException
     *             if an error occurs during statement parsing.
     */
    public synchronized List<AstTree> parseQuery(String stmt, String path, int offset_line, int offset_column)
            throws QueryParsingException
    {
        try
        {
            assert (stmt != null);
            
            long time = System.currentTimeMillis();
            
            QueryParser parser = getParser(stmt);
            
            List<AstTree> ast_trees = parser.parseQuery(stmt, path, offset_line, offset_column);
            
            if (log.isTraceEnabled())
            {
                log.trace("\n<---- Query String ---->\n" + stmt);
            }
            
            Timer.inc("Query Parsing Time", System.currentTimeMillis() - time);
            log.debug("Query Parsing Time: " + Timer.get("Query Parsing Time") + Timer.MS);
            // Timer.reset("Query Parsing Time");
            
            // No need to cleanup since no sources are accessed during parsing
            // this.cleanup(false);
            
            return ast_trees;
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Cleanup
            this.cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryParsingException) throw (QueryParsingException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    /**
     * Parses an input FPL statement and returns a list of AST trees.
     * 
     * @param stmt
     *            the FPL statement to parse.
     * @param location
     *            the location of the statement.
     * @return a list of AST trees.
     * @throws QueryParsingException
     *             if an error occurs during statement parsing.
     */
    public List<Definition> parseFplCode(String stmt, Location location) throws QueryParsingException
    {
        return this.parseFplCode(stmt, (location != null) ? location.getPath() : null, 0, 0);
    }
    
    /**
     * Parses an input FPL statement and returns a list of AST trees.
     * 
     * @param stmt
     *            the FPL statement to parse.
     * @param path
     *            path
     * @param offset_line
     *            offset_line
     * @param offset_column
     *            offset_column
     * @return a list of AST trees.
     * @throws QueryParsingException
     *             if an error occurs during statement parsing.
     */
    public synchronized List<Definition> parseFplCode(String stmt, String path, int offset_line, int offset_column)
            throws QueryParsingException
    {
        try
        {
            assert (stmt != null);
            
            long time = System.currentTimeMillis();
            
            QueryParser parser = getParser(stmt);
            
            List<Definition> ast_trees = parser.parseFplCode(stmt, path, offset_line, offset_column);
            
            if (log.isTraceEnabled())
            {
                log.trace("\n<---- FPL String ---->\n" + stmt);
            }
            
            Timer.inc("FPL Parsing Time", System.currentTimeMillis() - time);
            log.trace("FPL Parsing Time: " + Timer.get("FPL Parsing Time") + Timer.MS);
            // Timer.reset("FPL Parsing Time");
            
            // No need to cleanup since no sources are accessed during parsing
            // this.cleanup(false);
            
            return ast_trees;
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Cleanup
            this.cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryParsingException) throw (QueryParsingException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    /**
     * Parses an input action invocation and returns an AST node.
     * 
     * @param stmt
     *            the action invocation to parse.
     * @param location
     *            the location of the statement.
     * @return an AST node.
     * @throws QueryParsingException
     *             if an error occurs during statement parsing.
     */
    public ActionInvocation parseActionInvocation(String stmt, Location location) throws QueryParsingException
    {
        return this.parseActionInvocation(stmt, (location != null) ? location.getPath() : null, 0, 0);
    }
    
    /**
     * Parses an input action invocation and returns an AST node.
     * 
     * @param stmt
     *            the action statement to parse.
     * @param path
     *            path
     * @param offset_line
     *            offset_line
     * @param offset_column
     *            offset_column
     * @return an AST node.
     * @throws QueryParsingException
     *             if an error occurs during statement parsing.
     */
    public synchronized ActionInvocation parseActionInvocation(String stmt, String path, int offset_line, int offset_column)
            throws QueryParsingException
    {
        try
        {
            assert (stmt != null);
            
            long time = System.currentTimeMillis();
            
            QueryParser parser = getParser(stmt);
            
            ActionInvocation ast_node = parser.parseActionInvocation(stmt, path, offset_line, offset_column);
            
            if (log.isTraceEnabled())
            {
                log.trace("\n<---- FPL String ---->\n" + stmt);
            }
            
            Timer.inc("FPL Parsing Time", System.currentTimeMillis() - time);
            log.trace("FPL Parsing Time: " + Timer.get("FPL Parsing Time") + Timer.MS);
            // Timer.reset("FPL Parsing Time");
            
            // No need to cleanup since no sources are accessed during parsing
            // this.cleanup(false);
            
            return ast_node;
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Cleanup
            this.cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryParsingException) throw (QueryParsingException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    /**
     * Compiles the input AST trees and returns a list of physical plans.
     * 
     * @param ast_trees
     *            the AST trees to compile.
     * @param uas
     *            a unified application state.
     * @return a list of physical plans.
     * @throws QueryCompilationException
     *             if an error occurs during query compilation.
     */
    public List<PhysicalPlan> compile(List<AstTree> ast_trees, UnifiedApplicationState uas) throws QueryCompilationException
    {
        try
        {
            assert (ast_trees != null);
            
            this.setUnifiedApplicationState(uas);
            
            List<LogicalPlan> logical_plans = new ArrayList<LogicalPlan>();
            List<PhysicalPlan> results = null;
            
            // Construct the logical plan
            logical_plans = this.translate(ast_trees, m_uas);
            
            // Rewrite the logical plans
            logical_plans = this.rewriteSourceAgnostic(logical_plans, m_uas);
            
            // Distribute the logical plans to distributed normal form
            logical_plans = this.distribute(logical_plans, m_uas);
            
            // Rewrite the logical plans by applying source specific rewritings
            logical_plans = this.rewriteSourceSpecific(logical_plans, m_uas);
            
            // Generate the physical plans
            results = this.generate(logical_plans, m_uas);
            
            // Cleanup
            this.cleanup(false);
            
            return results;
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Cleanup
            this.cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryCompilationException) throw (QueryCompilationException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    /**
     * Translates the input AST trees and returns the initial logical plan.
     * 
     * @param ast_trees
     *            the AST trees to compile.
     * @param uas
     *            a unified application state.
     * @return a list of logical plans.
     * @throws QueryCompilationException
     *             if an error occurs during query compilation.
     */
    public List<LogicalPlan> translate(List<AstTree> ast_trees, UnifiedApplicationState uas) throws QueryCompilationException
    {
        try
        {
            assert (ast_trees != null);
            
            this.setUnifiedApplicationState(uas);
            
            List<LogicalPlan> results = new ArrayList<LogicalPlan>();
            
            long time = System.currentTimeMillis();
            
            for (AstTree ast_tree : ast_trees)
            {
                // Construct the logical plan
                LogicalPlan initial_logical_plan = LogicalPlanBuilder.build(ast_tree.getRoot(), m_uas);
                
                if (log.isDebugEnabled())
                {
                    log.debug("\n<---- Build Initial Logical Plan ---->\n" + initial_logical_plan.toExplainString());
                }
                
                results.add(initial_logical_plan);
            }
            
            Timer.inc("Query Translation Time", System.currentTimeMillis() - time);
            log.debug("\tQuery Translation Time: " + Timer.get("Query Translation Time") + Timer.MS);
            // Timer.reset("Query Translation Time");
            
            // Cleanup
            // this.cleanup(false);
            
            return results;
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Cleanup
            this.cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryCompilationException) throw (QueryCompilationException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    /**
     * Rewrites the input initial logical plans and returns a list of rewritten logical plans.
     * 
     * @param logical_plans
     *            input list of initial logical plans.
     * @param uas
     *            a unified application state.
     * @return a list of rewritten logical plans.
     * @throws QueryCompilationException
     *             if an error occurs during query compilation.
     */
    public List<LogicalPlan> rewriteSourceAgnostic(List<LogicalPlan> logical_plans, UnifiedApplicationState uas)
            throws QueryCompilationException
    {
        try
        {
            assert (logical_plans != null);
            
            this.setUnifiedApplicationState(uas);
            
            List<LogicalPlan> results = new ArrayList<LogicalPlan>();
            
            long time = System.currentTimeMillis();
            
            for (LogicalPlan logical_plan : logical_plans)
            {
                // Infer Keys for operators
                // KeyAnnotationBuilder builder = new KeyAnnotationBuilder(logical_plan);
                // builder.setCheckKeyInfoOfIntermediateResult(false);
                // builder.buildForPlan();
                // // Use retouched plan only if KeyInference occured no problems
                // if(builder.getRetouchedPlan() != null)
                // logical_plan = builder.getRetouchedPlan();
                
                // Teradata Pilot experimental optimizations
                
                // FIXME Romain: deactivated on branch for now.
                // if(Config.getOptimizationsPushDistinctAcrossUnion())
                // {
                // logical_plan = PushDistinctAcrossUnionRewriter.getInstance().rewrite(logical_plan);
                // }
                // if(Config.getOptimizationsPushSelectAcrossUnion())
                // {
                // logical_plan = PushSelectAcrossUnionRewriter.getInstance().rewrite(logical_plan);
                // }
                
                // Push selections down
                logical_plan = PushConditionsDownRewriter.getInstance(m_uas).rewrite(logical_plan);
                
                // Order the joins
                logical_plan = JoinOrderRewriter.getInstance(m_uas).rewrite(logical_plan);
                
                // // Build the index scan operators
                // logical_plan = IndexScanRewriter.getInstance(m_uas).rewrite(logical_plan);
                
                results.add(logical_plan);
                
                if (log.isDebugEnabled())
                {
                    log.debug("\n<---- Rewritten Logical Plan ---->\n" + logical_plan.toExplainString());
                    
                }
            }
            
            Timer.inc("Query Rewriting Time", System.currentTimeMillis() - time);
            log.debug("\t\tQuery Rewriting Time: " + Timer.get("Query Rewriting Time") + Timer.MS);
            // Timer.reset("Query Rewriting Time");
            
            // Cleanup
            this.cleanup(false);
            
            return results;
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Cleanup
            this.cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryCompilationException) throw (QueryCompilationException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    /**
     * Rewrites the input initial logical plans and returns a list of rewritten logical plans.
     * 
     * @param logical_plans
     *            input list of initial logical plans.
     * @param uas
     *            a unified application state.
     * @return a list of rewritten logical plans.
     * @throws QueryCompilationException
     *             if an error occurs during query compilation.
     */
    public List<LogicalPlan> rewriteSourceSpecific(List<LogicalPlan> logical_plans, UnifiedApplicationState uas)
            throws QueryCompilationException
    {
        try
        {
            assert (logical_plans != null);
            
            this.setUnifiedApplicationState(uas);
            
            List<LogicalPlan> results = new ArrayList<LogicalPlan>();
            
            long time = System.currentTimeMillis();
            
            for (LogicalPlan logical_plan : logical_plans)
            {
                for (SendPlan send_plan : LogicalPlanUtil.getAllSendPlans(logical_plan))
                {
                    String data_source_name = send_plan.getExecutionDataSourceName();
                    DataSource data_source = null;
                    try
                    {
                        data_source = m_uas.getDataSource(data_source_name);
                    }
                    catch (DataSourceException e)
                    {
                        throw new AssertionError(e);
                    }
                    
                    SourceWrapper wrapper = null;
                    switch (data_source.getMetaData().getStorageSystem())
                    {
                        case INMEMORY:
                            wrapper = new InMemorySourceWrapper();
                            break;
                        case JDBC:
                            wrapper = new SqlSourceWrapper();
                            break;
                        default:
                            break;
                    }
                    if (wrapper != null)
                    {
                        LogicalPlan plan = send_plan.getSendPlan();
                        wrapper.rewriteForSource(plan);
                    }
                }
                
                logical_plan.updateOutputInfoDeep();
                
                results.add(logical_plan);
                
                if (log.isDebugEnabled())
                {
                    log.debug("\n<---- Rewritten Logical Plan ---->\n" + logical_plan.toExplainString());
                    
                }
            }
            
            Timer.inc("Query Rewriting Time", System.currentTimeMillis() - time);
            log.debug("\t\tQuery Rewriting Time: " + Timer.get("Query Rewriting Time") + Timer.MS);
            // Timer.reset("Query Rewriting Time");
            
            // Cleanup
            this.cleanup(false);
            
            return results;
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Cleanup
            this.cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryCompilationException) throw (QueryCompilationException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    /**
     * Distributes the input logical plans and returns a list of logical plans in distributed normal form.
     * 
     * @param logical_plans
     *            input list of logical plans.
     * @param uas
     *            a unified application state.
     * @return a list of logical plans in distributed normal form.
     * @throws QueryCompilationException
     *             if an error occurs during query compilation.
     */
    public List<LogicalPlan> distribute(List<LogicalPlan> logical_plans, UnifiedApplicationState uas)
            throws QueryCompilationException
    {
        try
        {
            assert (logical_plans != null);
            
            this.setUnifiedApplicationState(uas);
            
            List<LogicalPlan> results = new ArrayList<LogicalPlan>();
            
            long time = System.currentTimeMillis();
            
            for (LogicalPlan logical_plan : logical_plans)
            {
                // Convert the logical plan to distributed normal form
                LogicalPlan dnf_logical_plan = LogicalPlanDistributor.getInstance(m_uas).distribute(logical_plan);
                
                if (log.isDebugEnabled())
                {
                    log.debug("\n<---- Distributed Logical Plan ---->\n" + dnf_logical_plan.toExplainString());
                    
                }
                
                results.add(dnf_logical_plan);
            }
            
            Timer.inc("Query Distribution Time", System.currentTimeMillis() - time);
            log.debug("\t\tQuery Distribution Time: " + Timer.get("Query Distribution Time") + Timer.MS);
            // Timer.reset("Query Distribution Time");
            
            // Cleanup
            this.cleanup(false);
            
            return results;
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Cleanup
            this.cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryCompilationException) throw (QueryCompilationException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    /**
     * Generates and returns a list of physical plans for the input list of distributed logical plans.
     * 
     * @param logical_plans
     *            input list of distributed logical plans.
     * @param uas
     *            a unified application state.
     * @return a list of physical plans.
     * @throws QueryCompilationException
     *             if an error occurs during query compilation.
     */
    public List<PhysicalPlan> generate(List<LogicalPlan> logical_plans, UnifiedApplicationState uas)
            throws QueryCompilationException
    {
        try
        {
            assert (logical_plans != null);
            this.setUnifiedApplicationState(uas);
            List<PhysicalPlan> results = new ArrayList<PhysicalPlan>();
            
            long time = System.currentTimeMillis();
            
            for (LogicalPlan dnf_logical_plan : logical_plans)
            {
                // Construct the physical plan
                PhysicalPlan physical_plan = PhysicalPlanBuilder.build(dnf_logical_plan, m_uas);
                
                if (log.isDebugEnabled())
                {
                    log.trace("\n<---- Physical Plan ---->\n" + physical_plan.toExplainString());
                }
                
                results.add(physical_plan);
            }
            
            Timer.inc("Query Generation Time", System.currentTimeMillis() - time);
            log.debug("\t\tQuery Generation Time: " + Timer.get("Query Generation Time") + Timer.MS);
            // Timer.reset("Query Generation Time");
            
            // Cleanup
            this.cleanup(false);
            
            return results;
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Cleanup
            this.cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryCompilationException) throw (QueryCompilationException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    /**
     * Constructs an eagerly executed query result. Note: The method assumes that the unified application state has been already
     * set.
     * 
     * @param physical_plan
     *            the physical plan to execute.
     * @param uas
     *            a unified application state.
     * @return a pipeline.
     * @throws QueryExecutionException
     * @exception QueryExecutionException
     *                if the execution of the input physical query plan raises an exception.
     */
    public EagerQueryResult createEagerQueryResult(PhysicalPlan physical_plan, UnifiedApplicationState uas)
            throws QueryExecutionException
    {
        long time = System.currentTimeMillis();
        if (!physical_plan.getLogicalPlan().isNested())
        {
            time = System.currentTimeMillis();
        }
        
        EagerQueryResult eager_result = new EagerQueryResult(createLazyQueryResult(physical_plan, uas));
        // try
        // {
        // eager_result.getValue();
        // }
        // catch (QueryExecutionException e)
        // {
        // assert (true);
        // }
        
        if (!physical_plan.getLogicalPlan().isNested())
        {
            Timer.inc("Query Execution Time", System.currentTimeMillis() - time);
            log.debug("\t\t\tQuery Execution Time: " + Timer.get("Query Execution Time") + Timer.MS);
            // Timer.reset("Query Execution Time");
        }
        
        return eager_result;
    }
    
    /**
     * Constructs returns a lazily executed query result that allows the retrieval of the resulting data tree in a pipelined
     * fashion.
     * 
     * @param physical_plan
     *            the physical plan to execute.
     * @param uas
     *            a unified application state.
     * @return a lazy query result.
     * @exception QueryExecutionException
     *                if the execution of the input physical query plan raises an exception.
     */
    public LazyQueryResult createLazyQueryResult(PhysicalPlan physical_plan, UnifiedApplicationState uas)
            throws QueryExecutionException
    {
        
        try
        {
            assert (physical_plan != null);
            
            // Copy the physical plan, if necessary
            long time = System.currentTimeMillis();
            PhysicalPlan copied_physical_plan = (physical_plan.isCopied()) ? physical_plan : physical_plan.copy(new CopyContext());
            // PhysicalPlan copied_physical_plan = physical_plan;
            // Timer.inc("Query Copy Time", System.currentTimeMillis() - time);
            // log.debug("\t\t\tQuery Copy Time: " + Timer.get("Query Copy Time") + Timer.MS);
            // Timer.reset("Query Copy Time");
            
            this.setUnifiedApplicationState(uas);
            
            QueryResult result = null;
            
            // FIXME Wrapping plans should not be supported.
            // assert (!copied_physical_plan.getLogicalPlan().isWrapping());
            
            // Non-nested plan
            if (!copied_physical_plan.getLogicalPlan().isNested())
            {
                // Make sure all resources are released before processing a non-nested plan
                if (!m_accessed_data_sources.isEmpty())
                {
                    log.error("Query processor found outstanding data source accesses." + m_accessed_data_sources.toString());
                    this.cleanup(true);
                }
            }
            
            DataSourceAccess access = null;
            String exec_data_source = null;
            
            // If the top operator is a send plan operator, then send the plan to the mediator
            if (copied_physical_plan.getRootOperatorImpl() instanceof SendPlanImpl)
            {
                exec_data_source = DataSource.MEDIATOR;
            }
            // Else, send the plan to the appropriate data source
            else
            {
                exec_data_source = copied_physical_plan.getLogicalPlan().getRootOperator().getExecutionDataSourceName();
            }
            
            // Retrieve the data source access
            access = this.getDataSourceAccess(exec_data_source);
            
            result = access.getDataSource().execute(copied_physical_plan, access.getTransaction());
            
            return (LazyQueryResult) result;
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Cleanup
            this.cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryExecutionException) throw (QueryExecutionException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    /**
     * Releases any resources allocated for the input physical plan.
     * 
     * @param exceptional
     *            determines whether the cleanup is caused by an exception.
     */
    public void cleanup(boolean exceptional)
    {
        try
        {
            for (DataSourceAccess access : m_accessed_data_sources.values())
            {
                if (exceptional)
                {
                    // Rollback the transaction
                    access.getTransaction().rollback();
                }
                else
                {
                    // Commit the transaction
                    access.getTransaction().commit();
                }
            }
            
            if (m_uas != null)
            {
                // Clean up the temp assign source
                // FIXME: The caller must guarantee the names in WITH clause must be unique among all the queries compiled in a
                // batch.
                m_uas.getDataSource(DataSource.TEMP_ASSIGN_SOURCE).clear();
            }
        }
        catch (QueryExecutionException e)
        {
            // This should not happen
            assert (true);
        }
        catch (DataSourceException e)
        {
            // This should not happen
            assert (true);
        }
        finally
        {
            m_accessed_data_sources.clear();
            m_param_insts.resetInstantiations();
        }
    }
    
    /**
     * Determines the output type of an AST tree.
     * 
     * @param ast_tree
     *            an AST tree.
     * @param uas
     *            a unified application state.
     * @return the output type of the AST tree.
     * @throws QueryCompilationException
     *             if an error occurs during query compilation.
     */
    public Type getOutputType(AstTree ast_tree, UnifiedApplicationState uas) throws QueryCompilationException
    {
        try
        {
            assert (ast_tree != null);
            
            this.setUnifiedApplicationState(uas);
            
            List<PhysicalPlan> results = compile(Collections.<AstTree> singletonList(ast_tree), m_uas);
            
            Type output_type = getOutputType(results.get(0).getLogicalPlan(), m_uas);
            
            // Cleanup
            this.cleanup(false);
            
            return output_type;
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Cleanup
            this.cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryCompilationException) throw (QueryCompilationException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    /**
     * Gets the output type of a physical plan.
     * 
     * @param logical_plan
     *            a logical plan.
     * @param uas
     *            a unified application state.
     * @return the output type of the logical plan.
     * @throws QueryCompilationException
     *             if an error occurs during query compilation.
     */
    public Type getOutputType(LogicalPlan logical_plan, UnifiedApplicationState uas) throws QueryCompilationException
    {
        try
        {
            assert (logical_plan != null);
            
            this.setUnifiedApplicationState(uas);
            
            Type output_type = logical_plan.getOutputType();
            
            // Unwrap the single attribute type of the collection tuple
            if (logical_plan.isWrapping())
            {
                assert (output_type instanceof CollectionType);
                TupleType tuple_type = ((CollectionType) output_type).getTupleType();
                assert (tuple_type.getSize() == 1);
                output_type = TypeUtil.cloneNoParent(tuple_type.iterator().next().getType());
            }
            
            // assert (output_type.checkConstraintConsistent());
            
            // Cleanup
            // this.cleanup(false);
            
            return output_type;
        }
        catch (Throwable t)
        {
            // Propagate abnormal conditions immediately
            if (t instanceof java.lang.Error && !(t instanceof AssertionError)) throw (java.lang.Error) t;
            
            // Cleanup
            this.cleanup(true);
            
            // Throw the expected exception
            if (t instanceof QueryCompilationException) throw (QueryCompilationException) t;
            
            // Propagate the unexpected exception
            throw (t instanceof UncheckedException) ? (UncheckedException) t : new UncheckedException(t);
        }
    }
    
    /**
     * Gets the meta data of a data source.
     * 
     * @param data_source_name
     *            the name of the data source.
     * @return a data source meta data.
     * @exception DataSourceException
     *                if a data source with the given name does not exist.
     */
    public DataSourceMetaData getDataSourceMetaData(String data_source_name) throws DataSourceException
    {
        if (m_uas == null)
        {
            m_uas = new UnifiedApplicationState(Collections.<DataSource> emptyList());
        }
        
        return m_uas.getDataSource(data_source_name).getMetaData();
    }
    
    /**
     * Gets the parameter instantiations used by the query processor.
     * 
     * @return the parameter instantiations used.
     */
    public ParameterInstantiations getParameterInstantiations()
    {
        return m_param_insts;
    }
    
    /**
     * Returns the data source access for a given data source name.
     * 
     * @param data_source_name
     *            the name of the data source.
     * @return a data source access.
     * @throws QueryExecutionException
     *             if there is an error executing a query.
     */
    public DataSourceAccess getDataSourceAccess(String data_source_name) throws QueryExecutionException
    {
        if (data_source_name.equals(DataSource.FPL_LOCAL_SCOPE))
        {
            // Do not cache the data access to the fpl local storage
            try
            {
                return new DataSourceAccess(DataSource.FPL_LOCAL_SCOPE);
            }
            catch (DataSourceException e)
            {
                // Chain the exception
                throw new QueryExecutionException(QueryExecution.DATA_SOURCE_ACCESS, e, data_source_name);
            }
        }
        
        if (!m_accessed_data_sources.containsKey(data_source_name))
        {
            try
            {
                m_accessed_data_sources.put(data_source_name, new DataSourceAccess(data_source_name));
            }
            catch (DataSourceException e)
            {
                // Chain the exception
                throw new QueryExecutionException(QueryExecution.DATA_SOURCE_ACCESS, e, data_source_name);
            }
        }
        
        return m_accessed_data_sources.get(data_source_name);
    }
    
    /**
     * Removes a data source access of a given data source name.
     * 
     * @param data_source_name
     *            the name of the data source
     * @throws QueryExecutionException
     *             if there is active transaction
     */
    public void removeDataSourceAccess(String data_source_name) throws QueryExecutionException
    {
        if (m_accessed_data_sources.containsKey(data_source_name))
        {
            DataSourceAccess access = m_accessed_data_sources.get(data_source_name);
            if (access.getTransaction().getState() == TransactionState.ACTIVE)
            {
                throw new QueryExecutionException(QueryExecution.REMOVE_DATASOURCE_WITH_ACTIVE_TRANSACTION, data_source_name);
            }
            m_accessed_data_sources.remove(data_source_name);
        }
    }
    
    /**
     * Holds all the objects needed by the query processor to efficiently communicate with a data source during query plan execution
     * and during cleanup.
     * 
     * @author Michalis Petropoulos
     * 
     */
    public final class DataSourceAccess
    {
        /**
         * The active transaction for the data source.
         */
        private DataSourceTransaction m_transaction;
        
        /**
         * Initializes an instance of the data source access.
         * 
         * @param data_source_name
         *            the name of the data source to access.
         * @throws DataSourceException
         *             if there is an error accessing the data source.
         * @throws QueryExecutionException
         *             if there is a query execution problem.
         */
        private DataSourceAccess(String data_source_name) throws DataSourceException, QueryExecutionException
        {
            assert (data_source_name != null);
            assert (m_uas != null);
            
            // Get the data source
            DataSource data_source = m_uas.getDataSource(data_source_name);
            
            // Begin a transaction
            m_transaction = data_source.beginTransaction();
        }
        
        /**
         * Returns the data source.
         * 
         * @return the data source.
         */
        public DataSource getDataSource()
        {
            return m_transaction.getDataSource();
        }
        
        /**
         * Returns the active transaction for the data source.
         * 
         * @return the active transaction for the data source.
         */
        public DataSourceTransaction getTransaction()
        {
            return m_transaction;
        }
    }
}
