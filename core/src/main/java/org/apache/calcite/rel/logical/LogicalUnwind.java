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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Unwind;
import org.apache.calcite.rex.RexInputRef;

import java.util.List;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Unwind} not
 * targeted at any particular engine or calling convention.
 */
public final class LogicalUnwind extends Unwind {
  private LogicalUnwind(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child, List<RexInputRef> inputs) {
    super(cluster, traitSet, child, inputs);
    assert traitSet.containsIfApplicable(Convention.NONE);
  }

  /**
   * Creates a LogicalUnwind by parsing serialized output.
   */
  public LogicalUnwind(RelInput input) {
    super(input);
  }

  /**
   * Creates a LogicalUnwind.
   *
   * @param child    Input relational expression
   * @param inputs   list of inputs to unwind
   */
  public static LogicalUnwind create(RelNode child, List<RexInputRef> inputs) {
    RelOptCluster cluster = child.getCluster();
    RelTraitSet traitSet =
        child.getTraitSet().replace(Convention.NONE);
    return new LogicalUnwind(cluster, traitSet, child, inputs);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public Unwind copy(RelTraitSet traitSet, RelNode newInput,
      List<RexInputRef> inputs) {
    return new LogicalUnwind(getCluster(), traitSet, newInput, inputs);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End LogicalUnwind.java
