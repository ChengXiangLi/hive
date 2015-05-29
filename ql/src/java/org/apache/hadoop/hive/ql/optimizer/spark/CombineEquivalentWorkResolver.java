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
package org.apache.hadoop.hive.ql.optimizer.spark;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalPlanResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SparkEdgeProperty;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.common.util.AnnotationUtils;

/**
 * CombineEquivalentWorkResolver would search inside SparkWork, and find and combine equivalent
 * works.
 */
public class CombineEquivalentWorkResolver implements PhysicalPlanResolver {
  protected static transient Log LOG = LogFactory.getLog(CombineEquivalentWorkResolver.class);

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    TaskGraphWalker taskWalker = new TaskGraphWalker(new EquivalentWorkMatcher());
    taskWalker.startWalking(topNodes, null);
    return pctx;
  }

  class EquivalentWorkMatcher implements Dispatcher {

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
      if (nd instanceof SparkTask) {
        // key: work, value: all the works equivalent as key.
        Map<BaseWork, Set<BaseWork>> equivalentWorks = Maps.newHashMap();
        SparkTask sparkTask = (SparkTask) nd;
        SparkWork sparkWork = sparkTask.getWork();
        BaseWork dummyWork = new MapWork();
        addDummyTask(dummyWork, sparkWork);
        while (matchEquivalentWork(dummyWork, sparkWork)) {
          // do nothing
        }
        removeDummyTask(dummyWork, sparkWork);
      }
      return null;
    }

    private void addDummyTask(BaseWork dummyWork, SparkWork sparkWork) {
      Set<BaseWork> roots = sparkWork.getRoots();
      sparkWork.add(dummyWork);
      for (BaseWork root : roots) {
        sparkWork.connect(dummyWork, root, new SparkEdgeProperty(0));
      }
    }

    private void removeDummyTask(BaseWork dummyWork, SparkWork sparkWork) {
      Set<BaseWork> roots = sparkWork.getRoots();
      for (BaseWork root : roots) {
        sparkWork.disconnect(dummyWork, root);
      }
      sparkWork.remove(dummyWork);
    }

    private boolean matchEquivalentWork(BaseWork rootWork, SparkWork sparkWork) {
      List<BaseWork> children = sparkWork.getChildren(rootWork);
        Set<Set<BaseWork>> equivalentWorks = compareChildWorks(children);
      if (isCombinableSet(equivalentWorks)) {
        mergeWorks(equivalentWorks, sparkWork);
        return true;
      } else {
        boolean result = false;
        for (BaseWork child : children) {
           result = result || matchEquivalentWork(child, sparkWork);
        }
        return result;
      }
    }

    private Set<Set<BaseWork>> compareChildWorks(List<BaseWork> children) {
      Set<Set<BaseWork>> equivalentChildren = Sets.newHashSet();
      if (children.size() > 1) {
        for (BaseWork work : children) {
          boolean assigned = false;
          for (Set<BaseWork> set : equivalentChildren) {
            if (belongToSet(set, work)) {
              set.add(work);
              assigned = true;
              break;
            }
          }
          if(!assigned) {
            Set<BaseWork> newSet = Sets.newHashSet();
            newSet.add(work);
            equivalentChildren.add(newSet);
          }
        }
      }
      return equivalentChildren;
    }

    private boolean isCombinableSet(Set<Set<BaseWork>> equivalentWorks) {
      boolean result = false;
      for (Set<BaseWork> workSet : equivalentWorks) {
        if (workSet.size() > 1) {
          result = true;
          break;
        }
      }
      return result;
    }

    private boolean belongToSet(Set<BaseWork> set, BaseWork work) {
      if (set.isEmpty()) {
        return true;
      } else if (compareWork(set.iterator().next(), work)) {
        return true;
      }
      return false;
    }

    private void mergeWorks(Set<Set<BaseWork>> equivalentWorks, SparkWork sparkWork) {
      for (Set<BaseWork> workSet : equivalentWorks) {
        if (workSet.size() > 1) {
          Iterator<BaseWork> iterator = workSet.iterator();
          BaseWork first = iterator.next();
          while (iterator.hasNext()) {
            BaseWork next = iterator.next();
            replaceWork(next, first, sparkWork);
          }
        }
      }
    }

    private void replaceWork(BaseWork previous, BaseWork current, SparkWork sparkWork) {
      List<BaseWork> parents = sparkWork.getParents(previous);
      List<BaseWork> children = sparkWork.getChildren(previous);
      for (BaseWork parent : parents) {
        SparkEdgeProperty edgeProperty = sparkWork.getEdgeProperty(parent, previous);
        sparkWork.disconnect(parent, previous);
        sparkWork.connect(parent, current, edgeProperty);
      }
      for (BaseWork child : children) {
        SparkEdgeProperty edgeProperty = sparkWork.getEdgeProperty(previous, child);
        sparkWork.disconnect(previous, child);
        sparkWork.connect(current, child, edgeProperty);
      }
      sparkWork.remove(previous);
    }

    private boolean compareWork(BaseWork first, BaseWork second) {

      if (!first.getClass().getName().equals(second.getClass().getName())) {
        return false;
      }

      Set<Operator<?>> firstRootOperators = first.getAllRootOperators();
      Set<Operator<?>> secondRootOperators = second.getAllRootOperators();
      if (firstRootOperators.size() != secondRootOperators.size()) {
        return false;
      }

      Iterator<Operator<?>> firstIterator = firstRootOperators.iterator();
      Iterator<Operator<?>> secondIterator = secondRootOperators.iterator();
      while (firstIterator.hasNext()) {
        boolean result = compareOperatorChain(firstIterator.next(), secondIterator.next());
        if (!result) {
          return result;
        }
      }

      return true;
    }

    private boolean compareOperatorChain(Operator<?> firstOperator, Operator<?> secondOperator) {
      boolean result = compareCurrentOperator(firstOperator, secondOperator);
      if (!result) {
        return result;
      }

      List<Operator<? extends OperatorDesc>> firstOperatorChildOperators = firstOperator.getChildOperators();
      List<Operator<? extends OperatorDesc>> secondOperatorChildOperators = secondOperator.getChildOperators();
      if (firstOperatorChildOperators == null && secondOperatorChildOperators != null) {
        return false;
      } else if (firstOperatorChildOperators != null && secondOperatorChildOperators == null) {
        return false;
      } else if (firstOperatorChildOperators != null && secondOperatorChildOperators != null) {
        if (firstOperatorChildOperators.size() != secondOperatorChildOperators.size()) {
          return false;
        }
        int size = firstOperatorChildOperators.size();
        for (int i = 0; i < size; i++) {
          result = compareOperatorChain(firstOperatorChildOperators.get(i), secondOperatorChildOperators.get(i));
          if (!result) {
            return false;
          }
        }
      }

      return true;
    }

    /**
     * Compare Operators through their Explain output string.
     *
     * @param firstOperator
     * @param secondOperator
     * @return
     */
    private boolean compareCurrentOperator(Operator<?> firstOperator, Operator<?> secondOperator) {
      if (!firstOperator.getClass().getName().equals(secondOperator.getClass().getName())) {
        return false;
      }

      Method[] methods = firstOperator.getConf().getClass().getMethods();
      for (Method m : methods) {
        Annotation note = AnnotationUtils.getAnnotation(m, Explain.class);

        if (note instanceof Explain) {
          Explain explain = (Explain) note;
          if (Explain.Level.EXTENDED.in(explain.explainLevels()) ||
            Explain.Level.DEFAULT.in(explain.explainLevels())) {
            Object firstObj = null;
            Object secondObj = null;
            try {
              firstObj = m.invoke(firstOperator.getConf());
            } catch (Exception e) {
              LOG.debug("Failed to get method return value.", e);
              firstObj = null;
            }

            try {
              secondObj = m.invoke(secondOperator.getConf());
            } catch (Exception e) {
              LOG.debug("Failed to get method return value.", e);
              secondObj = null;
            }

            if ((firstObj == null && secondObj != null) ||
              (firstObj != null && secondObj == null) ||
              (firstObj != null && secondObj != null &&
                !firstObj.toString().equals(secondObj.toString()))) {
              return false;
            }
          }
        }
      }
      return true;
    }
  }
}
