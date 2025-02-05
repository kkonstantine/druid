/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.aggregation.datasketches.quantiles;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class DoublesSketchAggregatorFactoryTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(DoublesSketchAggregatorFactory.class)
                  .withNonnullFields("name", "fieldName")
                  .withIgnoredFields("cacheTypeId")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(new NamedType(DoublesSketchAggregatorFactory.class, DoublesSketchModule.DOUBLES_SKETCH));
    final DoublesSketchAggregatorFactory factory = new DoublesSketchAggregatorFactory(
        "myFactory",
        "myField",
        1024,
        1000L
    );
    final byte[] json = mapper.writeValueAsBytes(factory);
    final DoublesSketchAggregatorFactory fromJson = (DoublesSketchAggregatorFactory) mapper.readValue(
        json,
        AggregatorFactory.class
    );
    Assert.assertEquals(factory, fromJson);
  }

  @Test
  public void testDefaultParams()
  {
    final DoublesSketchAggregatorFactory factory = new DoublesSketchAggregatorFactory(
        "myFactory",
        "myField",
        null,
        null
    );

    Assert.assertEquals(DoublesSketchAggregatorFactory.DEFAULT_K, factory.getK());
    Assert.assertEquals(DoublesSketchAggregatorFactory.DEFAULT_MAX_STREAM_LENGTH, factory.getMaxStreamLength());
  }

  @Test
  public void testMaxIntermediateSize()
  {
    DoublesSketchAggregatorFactory factory = new DoublesSketchAggregatorFactory(
        "myFactory",
        "myField",
        128,
        null
    );
    Assert.assertEquals(24608L, factory.getMaxIntermediateSize());

    factory = new DoublesSketchAggregatorFactory(
        "myFactory",
        "myField",
        128,
        1_000_000_000_000L
    );
    Assert.assertEquals(34848L, factory.getMaxIntermediateSize());
  }

  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(
                  new CountAggregatorFactory("count"),
                  new DoublesSketchAggregatorFactory("doublesSketch", "col", 8),
                  new DoublesSketchMergeAggregatorFactory("doublesSketchMerge", 8)
              )
              .postAggregators(
                  new FieldAccessPostAggregator("doublesSketch-access", "doublesSketch"),
                  new FinalizingFieldAccessPostAggregator("doublesSketch-finalize", "doublesSketch"),
                  new FieldAccessPostAggregator("doublesSketchMerge-access", "doublesSketchMerge"),
                  new FinalizingFieldAccessPostAggregator("doublesSketchMerge-finalize", "doublesSketchMerge")
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ValueType.LONG)
                    .add("doublesSketch", null)
                    .add("doublesSketchMerge", null)
                    .add("doublesSketch-access", ValueType.COMPLEX)
                    .add("doublesSketch-finalize", ValueType.LONG)
                    .add("doublesSketchMerge-access", ValueType.COMPLEX)
                    .add("doublesSketchMerge-finalize", ValueType.LONG)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
