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
package org.gradoop.flink.algorithms.gelly.pagerank.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.graph.library.linkanalysis.PageRank;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Key selector for PageRank.Result
 */
public class PageRankResultKeySelector
        implements KeySelector<PageRank.Result<GradoopId>, GradoopId> {

    /**
     * Selects GradoopId as key
     *
     * @param gradoopIdResult PageRank Algorithm result
     * @return selects gradoop id
     * @throws Exception
     */
    @Override
    public GradoopId getKey(PageRank.Result<GradoopId> gradoopIdResult) throws Exception {
        return gradoopIdResult.getVertexId0();
    }
}
