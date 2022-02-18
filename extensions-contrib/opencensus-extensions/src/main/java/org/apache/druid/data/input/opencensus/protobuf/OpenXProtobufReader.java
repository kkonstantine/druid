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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.opentelemetry.protobuf.OpenTelemetryMetricsProtobufReader;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.kafka.common.header.Header;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OpenXProtobufReader implements InputEntityReader
{

  private final OpenTelemetryMetricsProtobufReader openTelemetryMetricsProtobufReader;
  private final OpenCensusProtobufReader openCensusProtobufReader;
  private final InputEntityReader reader;
  public static final String VERSION_HEADER_KEY = "v";
  public static final int OPENTELEMETRY_FORMAT_VERSION = 1;
  public static final int OPENCENSUS_FORMAT_VERSION = 0;

  public OpenXProtobufReader(
      DimensionsSpec dimensionsSpec,
      InputEntity source,
      String metricDimension,
      String valueDimension,
      String metricLabelPrefix,
      String resourceLabelPrefix
  )
  {
    this (
      new OpenCensusProtobufReader(
        dimensionsSpec,
        (ByteEntity) source,
        metricDimension,
        metricLabelPrefix,
        resourceLabelPrefix
      ),
      new OpenTelemetryMetricsProtobufReader(
        dimensionsSpec,
        (ByteEntity) source,
        metricDimension,
        valueDimension,
        metricLabelPrefix,
        resourceLabelPrefix
      ));
    if (source instanceof KafkaRecordEntity) {
      KafkaRecordEntity kafkaInputEntity = (KafkaRecordEntity) source;
      Header versionHeader = kafkaInputEntity.getRecord().headers().lastHeader(VERSION_HEADER_KEY);
      if (versionHeader != null) {
        int version =
            ByteBuffer.wrap(versionHeader.value()).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (version == OPENTELEMETRY_FORMAT_VERSION) {
          reader = this.openTelemetryMetricsProtobufReader;
          return;
        }
      }
    }

    reader = new OpenCensusProtobufReader(
      dimensionsSpec,
      (ByteEntity) source,
      metricDimension,
      metricLabelPrefix,
      resourceLabelPrefix
    );
  }

  @VisibleForTesting
  OpenXProtobufReader(OpenCensusProtobufReader openCensusProtobufReader,
                      OpenTelemetryMetricsProtobufReader openTelemetryMetricsProtobufReader)
  {
    this.openCensusProtobufReader = openCensusProtobufReader;
    this.openTelemetryMetricsProtobufReader = openTelemetryMetricsProtobufReader;
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    return reader.read();
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return reader.sample();
  }
}
