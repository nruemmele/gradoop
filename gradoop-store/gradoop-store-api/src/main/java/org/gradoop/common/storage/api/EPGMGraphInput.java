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

package org.gradoop.common.storage.api;

import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;

/**
 * Definition of graph store input.
 * A graph input instance provide a set of writing methods for EPGM elements
 *
 * @param <GInput> graph head(output)
 * @param <VInput> graph vertex(output)
 * @param <EInput> graph edge(output)
 */
public interface EPGMGraphInput<
  GInput extends EPGMGraphHead,
  VInput extends EPGMVertex,
  EInput extends EPGMEdge> extends Closeable {

  /**
   * Writes the given graph data into the graph store.
   *
   * @param graphData graph data to write
   */
  void writeGraphHead(@Nonnull GInput graphData) throws IOException;

  /**
   * Writes the given vertex data into the graph store.
   *
   * @param vertexData vertex data to write
   */
  void writeVertex(@Nonnull VInput vertexData) throws IOException;

  /**
   * Writes the given edge data into the graph store.
   *
   * @param edgeData edge data to write
   */
  void writeEdge(@Nonnull EInput edgeData) throws IOException;

  /**
   * Setting this value to true, forces the store implementation to flush the
   * write buffers after every write.
   *
   * @param autoFlush true to enable auto flush, false to disable
   */
  void setAutoFlush(boolean autoFlush);

  /**
   * Flushes all buffered writes to the store.
   */
  void flush() throws IOException;

}
