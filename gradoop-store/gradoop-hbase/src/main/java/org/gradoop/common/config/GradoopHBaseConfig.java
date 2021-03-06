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
package org.gradoop.common.config;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.storage.config.GradoopStoreConfig;
import org.gradoop.common.storage.impl.hbase.api.EdgeHandler;
import org.gradoop.common.storage.impl.hbase.api.GraphHeadHandler;
import org.gradoop.common.storage.impl.hbase.api.PersistentEdgeFactory;
import org.gradoop.common.storage.impl.hbase.api.PersistentGraphHeadFactory;
import org.gradoop.common.storage.impl.hbase.api.PersistentVertexFactory;
import org.gradoop.common.storage.impl.hbase.api.VertexHandler;
import org.gradoop.common.storage.impl.hbase.constants.HBaseConstants;
import org.gradoop.common.storage.impl.hbase.factory.HBaseEdgeFactory;
import org.gradoop.common.storage.impl.hbase.factory.HBaseGraphHeadFactory;
import org.gradoop.common.storage.impl.hbase.factory.HBaseVertexFactory;
import org.gradoop.common.storage.impl.hbase.handler.HBaseEdgeHandler;
import org.gradoop.common.storage.impl.hbase.handler.HBaseGraphHeadHandler;
import org.gradoop.common.storage.impl.hbase.handler.HBaseVertexHandler;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Configuration class for using HBase with Gradoop.
 */
public class GradoopHBaseConfig extends GradoopStoreConfig<
  PersistentGraphHeadFactory<GraphHead>,
  PersistentVertexFactory<Vertex, Edge>,
  PersistentEdgeFactory<Edge, Vertex>> {


  /**
   * Graph table name.
   */
  private final TableName graphTableName;
  /**
   * EPGMVertex table name.
   */
  private final TableName vertexTableName;

  /**
   * EPGMEdge table name.
   */
  private final TableName edgeTableName;

  /**
   * Graph head handler.
   */
  private final GraphHeadHandler<GraphHead> graphHeadHandler;
  /**
   * EPGMVertex handler.
   */
  private final VertexHandler<Vertex, Edge> vertexHandler;
  /**
   * EPGMEdge handler.
   */
  private final EdgeHandler<Edge, Vertex> edgeHandler;

  /**
   * Creates a new Configuration.
   *
   * @param graphHeadHandler            graph head handler
   * @param vertexHandler               vertex handler
   * @param edgeHandler                 edge handler
   * @param env                                   flink execution environment
   * @param graphTableName              graph table name
   * @param vertexTableName             vertex table name
   * @param edgeTableName               edge table name
   */
  private GradoopHBaseConfig(
    GraphHeadHandler<GraphHead> graphHeadHandler,
    VertexHandler<Vertex, Edge> vertexHandler,
    EdgeHandler<Edge, Vertex> edgeHandler,
    ExecutionEnvironment env,
    String graphTableName,
    String vertexTableName,
    String edgeTableName
  ) {
    super(new HBaseGraphHeadFactory<>(),
      new HBaseVertexFactory<>(),
      new HBaseEdgeFactory<>(),
      env);
    checkArgument(!StringUtils.isEmpty(graphTableName),
      "Graph table name was null or empty");
    checkArgument(!StringUtils.isEmpty(vertexTableName),
      "EPGMVertex table name was null or empty");
    checkArgument(!StringUtils.isEmpty(edgeTableName),
      "EPGMEdge table name was null or empty");

    this.graphTableName = TableName.valueOf(graphTableName);
    this.vertexTableName = TableName.valueOf(vertexTableName);
    this.edgeTableName = TableName.valueOf(edgeTableName);

    this.graphHeadHandler = checkNotNull(graphHeadHandler, "GraphHeadHandler was null");
    this.vertexHandler = checkNotNull(vertexHandler, "VertexHandler was null");
    this.edgeHandler = checkNotNull(edgeHandler, "EdgeHandler was null");
  }

  /**
   * Creates a new Configuration.
   *
   * @param config          Gradoop configuration
   * @param graphTableName  graph table name
   * @param vertexTableName vertex table name
   * @param edgeTableName   edge table name
   */
  private GradoopHBaseConfig(
    GradoopHBaseConfig config,
    String graphTableName,
    String vertexTableName,
    String edgeTableName
  ) {
    this(config.getGraphHeadHandler(),
      config.getVertexHandler(),
      config.getEdgeHandler(),
      config.getExecutionEnvironment(),
      graphTableName,
      vertexTableName,
      edgeTableName);
  }

  /**
   * Creates a default Configuration using POJO handlers for vertices, edges
   * and graph heads and default table names.
   *
   * @param env apache flink execution environment
   * @return Default Gradoop HBase configuration.
   */
  public static GradoopHBaseConfig getDefaultConfig(ExecutionEnvironment env) {
    GraphHeadHandler<GraphHead> graphHeadHandler =
      new HBaseGraphHeadHandler<>(new GraphHeadFactory());
    VertexHandler<Vertex, Edge> vertexHandler =
      new HBaseVertexHandler<>(new VertexFactory());
    EdgeHandler<Edge, Vertex> edgeHandler =
      new HBaseEdgeHandler<>(new EdgeFactory());

    return new GradoopHBaseConfig(
      graphHeadHandler,
      vertexHandler,
      edgeHandler,
      env,
      HBaseConstants.DEFAULT_TABLE_GRAPHS,
      HBaseConstants.DEFAULT_TABLE_VERTICES,
      HBaseConstants.DEFAULT_TABLE_EDGES
    );
  }

  /**
   * Creates a Gradoop HBase configuration based on the given arguments.
   *
   * @param gradoopConfig   Gradoop configuration
   * @param graphTableName  graph table name
   * @param vertexTableName vertex table name
   * @param edgeTableName   edge table name
   *
   * @return Gradoop HBase configuration
   */
  public static GradoopHBaseConfig createConfig(
    GradoopHBaseConfig gradoopConfig,
    String graphTableName,
    String vertexTableName,
    String edgeTableName
  ) {
    return new GradoopHBaseConfig(gradoopConfig, graphTableName, vertexTableName, edgeTableName);
  }

  public TableName getVertexTableName() {
    return vertexTableName;
  }

  public TableName getEdgeTableName() {
    return edgeTableName;
  }

  public TableName getGraphTableName() {
    return graphTableName;
  }

  public GraphHeadHandler<GraphHead> getGraphHeadHandler() {
    return graphHeadHandler;
  }

  public VertexHandler<Vertex, Edge> getVertexHandler() {
    return vertexHandler;
  }

  public EdgeHandler<Edge, Vertex> getEdgeHandler() {
    return edgeHandler;
  }
}
