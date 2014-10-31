/**
 * 
 */
package edu.ucsd.forward.query.physical.visitor;

import edu.ucsd.forward.query.physical.AntiSemiJoinImplNestedLoop;
import edu.ucsd.forward.query.physical.ApplyPlanImpl;
import edu.ucsd.forward.query.physical.AssignImplJDBC;
import edu.ucsd.forward.query.physical.AssignMemoryToMemoryImpl;
import edu.ucsd.forward.query.physical.CopyImplInMemory;
import edu.ucsd.forward.query.physical.EliminateDuplicatesImpl;
import edu.ucsd.forward.query.physical.ExceptImpl;
import edu.ucsd.forward.query.physical.ExistsImpl;
import edu.ucsd.forward.query.physical.GroundImpl;
import edu.ucsd.forward.query.physical.GroupByImpl;
import edu.ucsd.forward.query.physical.IndexScanImpl;
import edu.ucsd.forward.query.physical.InnerJoinImplNestedLoop;
import edu.ucsd.forward.query.physical.IntersectImpl;
import edu.ucsd.forward.query.physical.NavigateImpl;
import edu.ucsd.forward.query.physical.OffsetFetchImpl;
import edu.ucsd.forward.query.physical.OuterJoinImplNestedLoop;
import edu.ucsd.forward.query.physical.OuterUnionImpl;
import edu.ucsd.forward.query.physical.PartitionByImpl;
import edu.ucsd.forward.query.physical.ProductImplNestedLoop;
import edu.ucsd.forward.query.physical.ProjectImpl;
import edu.ucsd.forward.query.physical.ScanImpl;
import edu.ucsd.forward.query.physical.ScanImplReferencing;
import edu.ucsd.forward.query.physical.SelectImplSeq;
import edu.ucsd.forward.query.physical.SemiJoinImplNestedLoop;
import edu.ucsd.forward.query.physical.SendPlanImplIdb;
import edu.ucsd.forward.query.physical.SendPlanImplInMemory;
import edu.ucsd.forward.query.physical.SendPlanImplJdbc;
import edu.ucsd.forward.query.physical.SendPlanImplRemote;
import edu.ucsd.forward.query.physical.SortImpl;
import edu.ucsd.forward.query.physical.SubqueryImpl;
import edu.ucsd.forward.query.physical.UnionImpl;
import edu.ucsd.forward.query.physical.ddl.CreateDataObjectImpl;
import edu.ucsd.forward.query.physical.ddl.DropDataObjectImpl;
import edu.ucsd.forward.query.physical.dml.DeleteImpl;
import edu.ucsd.forward.query.physical.dml.InsertImpl;
import edu.ucsd.forward.query.physical.dml.UpdateImpl;

/**
 * The interface of operator implementation visitor.
 * 
 * @author Yupeng
 * 
 */
public interface OperatorImplVisitor
{
    public void visitAntiSemiJoinImplNestedLoop(AntiSemiJoinImplNestedLoop operator_impl);
    
    public void visitApplyPlanImpl(ApplyPlanImpl operator_impl);
    
    public void visitCopyImplInMemory(CopyImplInMemory operator_impl);
    
    public void visitEliminateDuplicatesImpl(EliminateDuplicatesImpl operator_impl);
    
    public void visitExceptImpl(ExceptImpl operator_impl);
    
    public void visitExistsImpl(ExistsImpl operator_impl);
    
    public void visitGroundImpl(GroundImpl operator_impl);
    
    public void visitGroupByImpl(GroupByImpl operator_impl);
    
    public void visitInnerJoinImplNestedLoop(InnerJoinImplNestedLoop operator_impl);
    
    public void visitIntersectImpl(IntersectImpl operator_impl);
    
    public void visitNavigateImpl(NavigateImpl operator_impl);
    
    public void visitOffsetFetchImpl(OffsetFetchImpl operator_impl);
    
    public void visitOuterJoinImplNestedLoop(OuterJoinImplNestedLoop operator_impl);
    
    public void visitOuterUnionImpl(OuterUnionImpl operator_impl);
    
    public void visitPartitionByImpl(PartitionByImpl operator_impl);
    
    public void visitProductImplNestedLoop(ProductImplNestedLoop operator_impl);
    
    public void visitProjectImpl(ProjectImpl operator_impl);
    
    public void visitScanImpl(ScanImpl operator_impl);
    
    public void visitSelectImplSeq(SelectImplSeq operator_impl);
    
    public void visitSemiJoinImplNestedLoop(SemiJoinImplNestedLoop operator_impl);
    
    public void visitSendPlanImplInMemory(SendPlanImplInMemory operator_impl);
    
    public void visitSendPlanImplJdbc(SendPlanImplJdbc operator_impl);
    
    public void visitSendPlanImplIdb(SendPlanImplIdb operator_impl);
    
    public void visitSortImpl(SortImpl operator_impl);
    
    public void visitUnionImpl(UnionImpl operator_impl);
    
    public void visitCreateDataObjectImpl(CreateDataObjectImpl operator_impl);
    
    public void visitDropSchemaObjectImpl(DropDataObjectImpl operator_impl);
    
    public void visitInsertImpl(InsertImpl operator_impl);
    
    public void visitDeleteImpl(DeleteImpl operator_impl);
    
    public void visitUpdateImpl(UpdateImpl operator_impl);
    
    public void visitIndexScanImpl(IndexScanImpl operator_impl);
    
    public void visitSendPlanImplRemote(SendPlanImplRemote operator_impl);
    
    public void visitAssignImplJDBC(AssignImplJDBC assign_impl);
    
    public void visitAssignImpl(AssignMemoryToMemoryImpl assign_impl);
    
    public void visitScanImplReferencing(ScanImplReferencing scan_impl);
    
    public void visitSubqueryImpl(SubqueryImpl subqueryImpl);
}
