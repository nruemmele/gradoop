package org.gradoop.model.impl.operators.equality;

import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class EqualityByGraphIdsTest extends EqualityTestBase {

  @Test
  public void testExecute(){
    String asciiGraphs =
      "g1[(a)-[b]->(c)];g2[(a)-[b]->(c)];g3[(a)-[b]->(c)]";

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString(asciiGraphs);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c1
      = loader.getGraphCollectionByVariables("g1","g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c2
      = loader.getGraphCollectionByVariables("g1", "g2");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> c3
      = loader.getGraphCollectionByVariables("g1","g3");

    collectAndAssertEquals(
      new EqualityByGraphIds<GraphHeadPojo, VertexPojo, EdgePojo>()
        .execute(c1, c2));
    collectAndAssertNotEquals(
      new EqualityByGraphIds<GraphHeadPojo, VertexPojo, EdgePojo>()
        .execute(c1, c3));
  }
}