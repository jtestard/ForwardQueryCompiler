package edu.ucsd.forward.query.logical.visitors;

import edu.ucsd.forward.query.logical.AntiSemiJoin;
import edu.ucsd.forward.query.logical.ApplyPlan;
import edu.ucsd.forward.query.logical.Assign;
import edu.ucsd.forward.query.logical.Copy;
import edu.ucsd.forward.query.logical.EliminateDuplicates;
import edu.ucsd.forward.query.logical.Exists;
import edu.ucsd.forward.query.logical.Ground;
import edu.ucsd.forward.query.logical.GroupBy;
import edu.ucsd.forward.query.logical.IndexScan;
import edu.ucsd.forward.query.logical.InnerJoin;
import edu.ucsd.forward.query.logical.Navigate;
import edu.ucsd.forward.query.logical.OffsetFetch;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.OuterJoin;
import edu.ucsd.forward.query.logical.PartitionBy;
import edu.ucsd.forward.query.logical.Product;
import edu.ucsd.forward.query.logical.Project;
import edu.ucsd.forward.query.logical.Scan;
import edu.ucsd.forward.query.logical.Select;
import edu.ucsd.forward.query.logical.SemiJoin;
import edu.ucsd.forward.query.logical.SendPlan;
import edu.ucsd.forward.query.logical.SetOperator;
import edu.ucsd.forward.query.logical.Sort;
import edu.ucsd.forward.query.logical.Subquery;
import edu.ucsd.forward.query.logical.ddl.CreateDataObject;
import edu.ucsd.forward.query.logical.ddl.DropDataObject;
import edu.ucsd.forward.query.logical.dml.Delete;
import edu.ucsd.forward.query.logical.dml.Insert;
import edu.ucsd.forward.query.logical.dml.Update;

/**
 * The interface of an operator visitor.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface OperatorVisitor
{
    public Operator visitAntiSemiJoin(AntiSemiJoin operator);
    
    public Operator visitAssign(Assign operator);
    
    public Operator visitApplyPlan(ApplyPlan operator);
    
    public Operator visitCopy(Copy operator);
    
    public Operator visitEliminateDuplicates(EliminateDuplicates operator);
    
    public Operator visitExists(Exists operator);
    
    public Operator visitGround(Ground operator);
    
    public Operator visitGroupBy(GroupBy operator);
    
    public Operator visitInnerJoin(InnerJoin operator);
    
    public Operator visitNavigate(Navigate operator);
    
    public Operator visitOffsetFetch(OffsetFetch operator);
    
    public Operator visitOuterJoin(OuterJoin operator);
    
    public Operator visitPartitionBy(PartitionBy operator);
    
    public Operator visitProduct(Product operator);
    
    public Operator visitProject(Project operator);
    
    public Operator visitScan(Scan operator);
    
    public Operator visitSelect(Select operator);
    
    public Operator visitSemiJoin(SemiJoin operator);
    
    public Operator visitSendPlan(SendPlan operator);
    
    public Operator visitSetOperator(SetOperator operator);
    
    public Operator visitSort(Sort operator);
    
    public Operator visitCreateSchemaObject(CreateDataObject operator);
    
    public Operator visitDropSchemaObject(DropDataObject operator);
    
    public Operator visitInsert(Insert operator);
    
    public Operator visitDelete(Delete operator);
    
    public Operator visitUpdate(Update operator);
    
    public Operator visitIndexScan(IndexScan operator);

    public Operator visitSubquery(Subquery operator);
}
