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

package org.apache.druid.data.input.opencensus.protobuf;

import io.opencensus.proto.metrics.v1.Metric;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.opentelemetry.protobuf.OpenTelemetryMetricsProtobufReader;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;


public class OpenXProtobufReaderTest
{

  public static final byte[] OPENTELEMETRY_HEADER_BYTES = ByteBuffer.allocate(Integer.BYTES)
    .order(ByteOrder.LITTLE_ENDIAN)
    .putInt(OpenXProtobufReader.OPENTELEMETRY_FORMAT_VERSION)
    .array();
  public static final Headers OPENTELEMETRY_HEADER = new RecordHeaders(Collections.singleton(new RecordHeader(OpenXProtobufReader.VERSION_HEADER_KEY,
      OPENTELEMETRY_HEADER_BYTES)));
  public static final byte[] OPENCENSUS_HEADER_BYTES = ByteBuffer.allocate(Integer.BYTES)
    .order(ByteOrder.LITTLE_ENDIAN)
    .putInt(OpenXProtobufReader.OPENCENSUS_FORMAT_VERSION)
    .array();
  public static final Headers OPENCENSUS_HEADER = new RecordHeaders(Collections.singleton(new RecordHeader(OpenXProtobufReader.VERSION_HEADER_KEY,
      OPENCENSUS_HEADER_BYTES)));

  @Mock
  private OpenCensusProtobufReader openCensusProtobufReader;

  @Mock
  private OpenTelemetryMetricsProtobufReader openTelemetryMetricsProtobufReader;

  @Mock
  private KafkaRecordEntity opencensusKafkaRecordEntity;

  @Mock
  private KafkaRecordEntity opentelemetryKafkaRecordEntity;

  //@Mock
  //private MetricsData opentelemetryMetric;
  //@Mock
  //private Metric opecensusMetric;

  @Mock
  private OpenCensusProtobufInputFormat inputFormat;

  @Before
  public void setUp() {

  //    when(ocensusSchemaAndValue.schema()).thenReturn(openCensusProtobufReader);
    when(opencensusKafkaRecordEntity.getRecord().headers()).thenReturn(OPENCENSUS_HEADER);
  //    when(opencensusKafkaRecordEntity.getBuffer()).thenReturn(opecensusMetric);

    when(opentelemetryKafkaRecordEntity.getRecord().headers()).thenReturn(OPENTELEMETRY_HEADER);
  //    when(opentelemetryKafkaRecordEntity.getBuffer()).thenReturn(opentelemetryMetric);

    when(inputFormat.createReader(ArgumentMatchers.any(), opencensusKafkaRecordEntity,
      null)).thenReturn(openCensusProtobufReader);
    when(inputFormat.createReader(ArgumentMatchers.any(), opentelemetryKafkaRecordEntity,
      null)).thenReturn(openTelemetryMetricsProtobufReader);

  }


  @Test
  public void testEmptyHeaderCreateReader()
  {
//    OpenCensusProtobufInputFormat inputFormat = new OpenCensusProtobufInputFormat("metric.name", null, "descriptor.", "custom.");
//    CloseableIterator<InputRow> rows = inputFormat.createReader(new InputRowSchema(
//      new TimestampSpec("timestamp", "iso", null),
//      DIMENSIONSSPEC,
//      Collections.emptyList()
//    ), kafkaRecordEntity, null).read();
//
//    ConsumerRecord emptyHeaderRecord =
//        new ConsumerRecord(OpenTelemetryInputFormatTest.TOPIC,
//          OpenTelemetryInputFormatTest.PARTITION,
//          OpenTelemetryInputFormatTest.OFFSET,
//        OpenTelemetryInputFormatTest.TS, OpenTelemetryInputFormatTest.TSTYPE,
//        -1L, -1, -1, null, null, new RecordHeaders());
//
//    OpenXProtobufReader openXProtobufReader =
//        new OpenXProtobufReader(OpenTelemetryInputFormatTest.DIMENSIONSSPEC,
//        new KafkaRecordEntity(emptyHeaderRecord),
//        "metric.name",
//        null, "descriptor.", "custom.");
//
//    assertThat(openXProtobufReader, instanceOf(OpenCensusProtobufReader.class));
    assertThat(inputFormat.);
  }

  @Test
  public void testOpencensusFormatCreateReader()
  {

    ConsumerRecord opencensusRecord =
        new ConsumerRecord(OpenTelemetryInputFormatTest.TOPIC,
          OpenTelemetryInputFormatTest.PARTITION,
          OpenTelemetryInputFormatTest.OFFSET,
          OpenTelemetryInputFormatTest.TS, OpenTelemetryInputFormatTest.TSTYPE,
        -1L, -1, -1, null, null, OpenXProtobufReaderTest.OPENCENSUS_HEADER);

    OpenXProtobufReader openXProtobufReader =
        new OpenXProtobufReader(OpenTelemetryInputFormatTest.DIMENSIONSSPEC,
        new KafkaRecordEntity(opencensusRecord),
        "metric.name",
        null, "descriptor.", "custom.");

    assertThat(openXProtobufReader, instanceOf(OpenCensusProtobufReader.class));
  }

  @Test
  public void testOpentelemetryFormatCreateReader()
  {

    ConsumerRecord opentelemetryRecord =
            new ConsumerRecord(OpenTelemetryInputFormatTest.TOPIC,
            OpenTelemetryInputFormatTest.PARTITION,
            OpenTelemetryInputFormatTest.OFFSET,
            OpenTelemetryInputFormatTest.TS, OpenTelemetryInputFormatTest.TSTYPE,
        -1L, -1, -1, null, null, OpenXProtobufReaderTest.OPENTELEMETRY_HEADER);

    OpenXProtobufReader openXProtobufReader =
            new OpenXProtobufReader(OpenTelemetryInputFormatTest.DIMENSIONSSPEC,
            new KafkaRecordEntity(opentelemetryRecord),
            "metric.name",
        null, "descriptor.", "custom.");

    assertThat(openXProtobufReader, instanceOf(OpenTelemetryMetricsProtobufReader.class));
  }

}
