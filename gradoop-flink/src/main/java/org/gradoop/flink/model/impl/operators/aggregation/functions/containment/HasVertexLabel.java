/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.aggregation.functions.containment;

import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

/**
 * Aggregate and filter function to check presence of a vertex label in a graph.
 *
 * Usage: First, aggregate and, second, filter using the same UDF instance.
 */
public class HasVertexLabel
  extends HasLabel implements VertexAggregateFunction {

  /**
   * Constructor.
   *
   * @param label vertex label to check presence of
   */
  public HasVertexLabel(String label) {
    super(label);
  }

  @Override
  public PropertyValue getVertexIncrement(Vertex vertex) {
    return PropertyValue.create(vertex.getLabel().equals(label));
  }

  @Override
  public String getAggregatePropertyKey() {
    return "hasVertexLabel_" + label;
  }

}
