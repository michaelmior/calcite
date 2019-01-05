/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.core;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;

import java.util.List;

/**
 * Relational expression that unwinds one or more inputs into multiple rows.
 */
public abstract class Unwind extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  protected final List<RexInputRef> inputs;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an Unwind.

   * @param cluster  Cluster that this relational expression belongs to
   * @param traits   Traits of this relational expression
   * @param child    Input relational expression
   * @param inputs   list of inputs to unwind
   */
  public Unwind(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      List<RexInputRef> inputs) {
    super(cluster, traits, child);
    this.inputs = inputs;
  }

  /**
   * Creates an Unwind by parsing serialized output.
   */
  public Unwind(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput(),
        (List<RexInputRef>) input.getInputList("inputs"));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final Unwind copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), this.inputs);
  }

  public abstract Unwind copy(RelTraitSet traitSet, RelNode newInput, List<RexInputRef> inputs);

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double rowCount = mq.getRowCount(this);
    List<Double> sizes = mq.getAverageColumnSizesNotNull(getInput());
    for (RexInputRef input : this.inputs) {
      final Double inputSize = sizes.get(input.getIndex());
      final Double typeSize = RelMdUtil.averageTypeValueSize(input.getType());
      if (inputSize != null && typeSize != null) {
        rowCount *= inputSize / typeSize;
      } else {
        rowCount *= 2;
      }
    }
    return planner.getCostFactory().makeCost(rowCount, rowCount, 0);
  }

  /**
   * Returns the inputs that will be unwound.
   */
  public List<RexInputRef> getUnwind() {
    return inputs;
  }
}

// End Unwind.java
