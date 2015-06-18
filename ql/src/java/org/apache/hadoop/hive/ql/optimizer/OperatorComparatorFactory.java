/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.exec.CollectOperator;
import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.DemuxOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.ForwardOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.HashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewForwardOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.ListSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MuxOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.SparkHashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.TemporaryHashSinkOperator;
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorFilterOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorLimitOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorSelectOperator;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.HashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.LateralViewJoinDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.ScriptDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.SparkHashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;

public class OperatorComparatorFactory {
  private static final Map<Class<?>, OperatorComparator> comparatorMapping = Maps.newHashMap();

  static {
    comparatorMapping.put(TableScanOperator.class, new TableScanOperatorComparator());
    comparatorMapping.put(SelectOperator.class, new SelectOperatorComparator());
    comparatorMapping.put(FilterOperator.class, new FilterOperatorComparator());
    comparatorMapping.put(GroupByOperator.class, new GroupByOperatorComparator());
    comparatorMapping.put(ReduceSinkOperator.class, new ReduceSinkOperatorComparator());
    comparatorMapping.put(FileSinkOperator.class, new FileSinkOperatorComparator());
    comparatorMapping.put(UnionOperator.class, new UnionOperatorComparator());
    comparatorMapping.put(JoinOperator.class, new JoinOperatorComparator());
    comparatorMapping.put(MapJoinOperator.class, new MapJoinOperatorComparator());
    comparatorMapping.put(SMBMapJoinOperator.class, new SMBMapJoinOperatorComparator());
    comparatorMapping.put(LimitOperator.class, new LimitOperatorComparator());
    comparatorMapping.put(SparkHashTableSinkOperator.class, new SparkHashTableSinkOperatorComparator());
    comparatorMapping.put(LateralViewJoinOperator.class, new LateralViewJoinOperatorComparator());
    comparatorMapping.put(VectorGroupByOperator.class, new GroupByOperatorComparator());
    comparatorMapping.put(CommonMergeJoinOperator.class, new MapJoinOperatorComparator());
    comparatorMapping.put(VectorFilterOperator.class, new FilterOperatorComparator());
    comparatorMapping.put(UDTFOperator.class, new UDTFOperatorComparator());
    comparatorMapping.put(VectorSelectOperator.class, new SelectOperatorComparator());
    comparatorMapping.put(VectorLimitOperator.class, new LimitOperatorComparator());
    comparatorMapping.put(ScriptOperator.class, new ScriptOperatorComparator());
    comparatorMapping.put(TemporaryHashSinkOperator.class, new HashTableSinkOperatorComparator());
    // these operators does not have state, so they always equal with the same kind.
    comparatorMapping.put(ForwardOperator.class, new AlwaysTrueOperatorComparator());
    comparatorMapping.put(LateralViewForwardOperator.class, new AlwaysTrueOperatorComparator());
    comparatorMapping.put(DemuxOperator.class, new AlwaysTrueOperatorComparator());
    comparatorMapping.put(MuxOperator.class, new AlwaysTrueOperatorComparator());
    comparatorMapping.put(ListSinkOperator.class, new AlwaysTrueOperatorComparator());
    comparatorMapping.put(CollectOperator.class, new AlwaysTrueOperatorComparator());
    // do not support PTFOperator comparing now.
    comparatorMapping.put(PTFOperator.class, new AlwaysFalseOperatorComparator());
  }

  public static OperatorComparator getOperatorComparator(Class<? extends Operator> operatorClass) {
    OperatorComparator operatorComparator = comparatorMapping.get(operatorClass);
    if (operatorComparator == null) {
      throw new RuntimeException("No OperatorComparator is registered for " + operatorClass.getName() + "yet.");
    }

    return operatorComparator;
  }

  public static interface OperatorComparator<T extends Operator<?>> {
    public boolean equals(T op1, T op2);
  }

  static class AlwaysTrueOperatorComparator implements OperatorComparator<Operator<?>> {

    @Override
    public boolean equals(Operator<?> op1, Operator<?> op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      return true;
    }
  }

  static class AlwaysFalseOperatorComparator implements OperatorComparator<Operator<?>> {

    @Override
    public boolean equals(Operator<?> op1, Operator<?> op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      return false;
    }
  }

  static class TableScanOperatorComparator implements OperatorComparator<TableScanOperator> {

    @Override
    public boolean equals(TableScanOperator op1, TableScanOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      TableScanDesc op1Conf = op1.getConf();
      TableScanDesc op2Conf = op2.getConf();

      boolean result = true;
      result = result && compareString(op1Conf.getAlias(), op2Conf.getAlias());
      result = result && compareExprNodeDesc(op1Conf.getFilterExpr(), op2Conf.getFilterExpr());
      result = result && (op1Conf.getRowLimit() == op2Conf.getRowLimit());
      result = result && (op1Conf.isGatherStats() == op2Conf.isGatherStats());

      return result;
    }
  }

  static class SelectOperatorComparator implements OperatorComparator<SelectOperator> {

    @Override
    public boolean equals(SelectOperator op1, SelectOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      SelectDesc op1Conf = op1.getConf();
      SelectDesc op2Conf = op2.getConf();

      boolean result = true;
      result = result && compareString(op1Conf.getColListString(), op2Conf.getColListString());
      result = result && compareObject(op1Conf.getOutputColumnNames(), op2Conf.getOutputColumnNames());
      result = result && compareString(op1Conf.explainNoCompute(), op2Conf.explainNoCompute());

      return result;
    }
  }

  static class FilterOperatorComparator implements OperatorComparator<FilterOperator> {

    @Override
    public boolean equals(FilterOperator op1, FilterOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      FilterDesc op1Conf = op1.getConf();
      FilterDesc op2Conf = op2.getConf();

      boolean result = true;
      result = result && compareString(op1Conf.getPredicateString(), op2Conf.getPredicateString());
      result = result && (op1Conf.getIsSamplingPred() == op2Conf.getIsSamplingPred());
      result = result && compareString(op1Conf.getSampleDescExpr(), op2Conf.getSampleDescExpr());

      return result;
    }
  }

  static class GroupByOperatorComparator implements OperatorComparator<GroupByOperator> {

    @Override
    public boolean equals(GroupByOperator op1, GroupByOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      GroupByDesc op1Conf = op1.getConf();
      GroupByDesc op2Conf = op2.getConf();

      boolean result = true;
      result = result && compareString(op1Conf.getModeString(), op2Conf.getModeString());
      result = result && compareString(op1Conf.getKeyString(), op2Conf.getKeyString());
      result = result && compareObject(op1Conf.getOutputColumnNames(), op2Conf.getOutputColumnNames());
      result = result && (op1Conf.pruneGroupingSetId() == op2Conf.pruneGroupingSetId());
      result = result && compareObject(op1Conf.getAggregatorStrings(), op2Conf.getAggregatorStrings());
      result = result && (op1Conf.getBucketGroup() == op2Conf.getBucketGroup());

      return result;
    }
  }

  static class ReduceSinkOperatorComparator implements OperatorComparator<ReduceSinkOperator> {

    @Override
    public boolean equals(ReduceSinkOperator op1, ReduceSinkOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      ReduceSinkDesc op1Conf = op1.getConf();
      ReduceSinkDesc op2Conf = op2.getConf();

      boolean result = true;
      result = result && compareExprNodeDescList(op1Conf.getKeyCols(), op2Conf.getKeyCols());
      result = result && compareExprNodeDescList(op1Conf.getValueCols(), op2Conf.getValueCols());
      result = result && compareExprNodeDescList(op1Conf.getPartitionCols(), op2Conf.getPartitionCols());
      result = result && (op1Conf.getTag() == op2Conf.getTag());
      result = result && compareString(op1Conf.getOrder(), op2Conf.getOrder());
      result = result && (op1Conf.getTopN() == op2Conf.getTopN());
      result = result && (op1Conf.isAutoParallel() == op2Conf.isAutoParallel());

      return result;
    }
  }

  static class FileSinkOperatorComparator implements OperatorComparator<FileSinkOperator> {

    @Override
    public boolean equals(FileSinkOperator op1, FileSinkOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      FileSinkDesc op1Conf = op1.getConf();
      FileSinkDesc op2Conf = op2.getConf();

      boolean result = true;
      result = result && compareObject(op1Conf.getDirName(), op2Conf.getDirName());
      result = result && compareObject(op1Conf.getTableInfo(), op2Conf.getTableInfo());
      result = result && (op1Conf.getCompressed() == op2Conf.getCompressed());
      result = result && (op1Conf.getDestTableId() == op2Conf.getDestTableId());
      result = result && (op1Conf.isMultiFileSpray() == op2Conf.isMultiFileSpray());
      result = result && (op1Conf.getTotalFiles() == op2Conf.getTotalFiles());
      result = result && (op1Conf.getNumFiles() == op2Conf.getNumFiles());
      result = result && compareString(op1Conf.getStaticSpec(), op2Conf.getStaticSpec());
      result = result && (op1Conf.isGatherStats() == op2Conf.isGatherStats());
      result = result && compareString(op1Conf.getStatsAggPrefix(), op2Conf.getStatsAggPrefix());

      return result;
    }
  }

  static class UnionOperatorComparator implements OperatorComparator<UnionOperator> {

    @Override
    public boolean equals(UnionOperator op1, UnionOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      return true;
    }
  }

  static class JoinOperatorComparator implements OperatorComparator<JoinOperator> {

    @Override
    public boolean equals(JoinOperator op1, JoinOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      JoinDesc desc1 = op1.getConf();
      JoinDesc desc2 = op2.getConf();

      boolean result = true;
      result = result && compareObject(desc1.getKeysString(), desc2.getKeysString());
      result = result && compareObject(desc1.getFiltersStringMap(), desc2.getFiltersStringMap());
      result = result && compareObject(desc1.getOutputColumnNames(), desc2.getOutputColumnNames());
      result = result && compareObject(desc1.getCondsList(), desc2.getCondsList());
      result = result && (desc1.getHandleSkewJoin() == desc2.getHandleSkewJoin());
      result = result && compareString(desc1.getNullSafeString(), desc2.getNullSafeString());

      return result;
    }
  }

  static class MapJoinOperatorComparator implements OperatorComparator<MapJoinOperator> {

    @Override
    public boolean equals(MapJoinOperator op1, MapJoinOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      MapJoinDesc desc1 = op1.getConf();
      MapJoinDesc desc2 = op2.getConf();

      boolean result = true;
      result = result && compareObject(desc1.getParentToInput(), desc2.getParentToInput());
      result = result && compareString(desc1.getKeyCountsExplainDesc(), desc2.getKeyCountsExplainDesc());
      result = result && compareObject(desc1.getKeysString(), desc2.getKeysString());
      result = result && (desc1.getPosBigTable() == desc2.getPosBigTable());
      result = result && (desc1.isBucketMapJoin() == desc2.isBucketMapJoin());

      result = result && compareObject(desc1.getKeysString(), desc2.getKeysString());
      result = result && compareObject(desc1.getFiltersStringMap(), desc2.getFiltersStringMap());
      result = result && compareObject(desc1.getOutputColumnNames(), desc2.getOutputColumnNames());
      result = result && compareObject(desc1.getCondsList(), desc2.getCondsList());
      result = result && (desc1.getHandleSkewJoin() == desc2.getHandleSkewJoin());
      result = result && compareString(desc1.getNullSafeString(), desc2.getNullSafeString());

      return result;
    }
  }

  static class SMBMapJoinOperatorComparator implements OperatorComparator<SMBMapJoinOperator> {

    @Override
    public boolean equals(SMBMapJoinOperator op1, SMBMapJoinOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      SMBJoinDesc desc1 = op1.getConf();
      SMBJoinDesc desc2 = op2.getConf();

      boolean result = true;
      result = result && compareObject(desc1.getParentToInput(), desc2.getParentToInput());
      result = result && compareString(desc1.getKeyCountsExplainDesc(), desc2.getKeyCountsExplainDesc());
      result = result && compareObject(desc1.getKeysString(), desc2.getKeysString());
      result = result && (desc1.getPosBigTable() == desc2.getPosBigTable());
      result = result && (desc1.isBucketMapJoin() == desc2.isBucketMapJoin());

      result = result && compareObject(desc1.getKeysString(), desc2.getKeysString());
      result = result && compareObject(desc1.getFiltersStringMap(), desc2.getFiltersStringMap());
      result = result && compareObject(desc1.getOutputColumnNames(), desc2.getOutputColumnNames());
      result = result && compareObject(desc1.getCondsList(), desc2.getCondsList());
      result = result && (desc1.getHandleSkewJoin() == desc2.getHandleSkewJoin());
      result = result && compareString(desc1.getNullSafeString(), desc2.getNullSafeString());

      return result;
    }
  }

  static class LimitOperatorComparator implements OperatorComparator<LimitOperator> {

    @Override
    public boolean equals(LimitOperator op1, LimitOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      LimitDesc desc1 = op1.getConf();
      LimitDesc desc2 = op2.getConf();

      return desc1.getLimit() == desc2.getLimit();
    }
  }

  static class SparkHashTableSinkOperatorComparator implements OperatorComparator<SparkHashTableSinkOperator> {

    @Override
    public boolean equals(SparkHashTableSinkOperator op1, SparkHashTableSinkOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      SparkHashTableSinkDesc desc1 = op1.getConf();
      SparkHashTableSinkDesc desc2 = op2.getConf();

      boolean result = true;
      result = result && compareObject(desc1.getFilterMapString(), desc2.getFilterMapString());
      result = result && compareObject(desc1.getKeysString(), desc2.getKeysString());
      result = result && (desc1.getPosBigTable() == desc2.getPosBigTable());

      result = result && compareObject(desc1.getKeysString(), desc2.getKeysString());
      result = result && compareObject(desc1.getFiltersStringMap(), desc2.getFiltersStringMap());
      result = result && compareObject(desc1.getOutputColumnNames(), desc2.getOutputColumnNames());
      result = result && compareObject(desc1.getCondsList(), desc2.getCondsList());
      result = result && (desc1.getHandleSkewJoin() == desc2.getHandleSkewJoin());
      result = result && compareString(desc1.getNullSafeString(), desc2.getNullSafeString());

      return result;
    }
  }

  static class HashTableSinkOperatorComparator implements OperatorComparator<HashTableSinkOperator> {

    @Override
    public boolean equals(HashTableSinkOperator op1, HashTableSinkOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      HashTableSinkDesc desc1 = op1.getConf();
      HashTableSinkDesc desc2 = op2.getConf();

      boolean result = true;
      result = result && compareObject(desc1.getFilterMapString(), desc2.getFilterMapString());
      result = result && compareObject(desc1.getKeysString(), desc2.getKeysString());
      result = result && (desc1.getPosBigTable() == desc2.getPosBigTable());

      result = result && compareObject(desc1.getKeysString(), desc2.getKeysString());
      result = result && compareObject(desc1.getFiltersStringMap(), desc2.getFiltersStringMap());
      result = result && compareObject(desc1.getOutputColumnNames(), desc2.getOutputColumnNames());
      result = result && compareObject(desc1.getCondsList(), desc2.getCondsList());
      result = result && (desc1.getHandleSkewJoin() == desc2.getHandleSkewJoin());
      result = result && compareString(desc1.getNullSafeString(), desc2.getNullSafeString());

      return result;
    }
  }

  static class LateralViewJoinOperatorComparator implements OperatorComparator<LateralViewJoinOperator> {

    @Override
    public boolean equals(LateralViewJoinOperator op1, LateralViewJoinOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      LateralViewJoinDesc desc1 = op1.getConf();
      LateralViewJoinDesc desc2 = op2.getConf();

      return compareObject(desc1.getOutputInternalColNames(), desc2.getOutputInternalColNames());
    }
  }

  static class ScriptOperatorComparator implements OperatorComparator<ScriptOperator> {

    @Override
    public boolean equals(ScriptOperator op1, ScriptOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      ScriptDesc desc1 = op1.getConf();
      ScriptDesc desc2 = op2.getConf();

      boolean result = true;
      result = result && compareString(desc1.getScriptCmd(), desc2.getScriptCmd());
      result = result && compareObject(desc1.getScriptOutputInfo(), desc2.getScriptOutputInfo());

      return result;
    }
  }

  static class UDTFOperatorComparator implements OperatorComparator<UDTFOperator> {

    @Override
    public boolean equals(UDTFOperator op1, UDTFOperator op2) {
      Preconditions.checkNotNull(op1);
      Preconditions.checkNotNull(op2);
      UDTFDesc desc1 = op1.getConf();
      UDTFDesc desc2 = op2.getConf();

      boolean result = true;
      result = result && compareString(desc1.getUDTFName(), desc2.getUDTFName());
      result = result && compareString(desc1.isOuterLateralView(), desc2.isOuterLateralView());

      return result;
    }
  }

  static boolean compareString(String first, String second) {
    return compareObject(first, second);
  }

  /*
   * Compare Objects which implements its own meaningful equals methods.
   */
  static boolean compareObject(Object first, Object second) {
    return first == null ? second == null : first.equals(second);
  }

  static boolean compareExprNodeDesc(ExprNodeDesc first, ExprNodeDesc second) {
    return first == null ? second == null : first.isSame(second);
  }

  static boolean compareExprNodeDescList(List<ExprNodeDesc> first, List<ExprNodeDesc> second) {
    if (first == null && second == null) {
      return true;
    }
    if ((first == null && second != null) || (first != null && second == null)) {
      return false;
    }
    if (first.size() != second.size()) {
      return false;
    } else {
      for (int i = 0; i < first.size(); i++) {
        if (!first.get(i).isSame(second.get(i))) {
          return false;
        }
      }
    }
    return true;
  }
}