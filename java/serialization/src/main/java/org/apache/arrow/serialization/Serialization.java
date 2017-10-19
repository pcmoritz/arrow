/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*

import java.io.ByteArrayInputStream;
import org.apache.arrow.vector.stream.ArrowStreamReader;
import org.apache.arrow.memory.RootAllocator;

// /env -class-path /Users/pcmoritz/.m2/repository/org/apache/arrow/arrow-vector/0.8.0-SNAPSHOT/arrow-vector-0.8.0-SNAPSHOT.jar:/Users/pcmoritz/.m2/repository/org/apache/arrow/arrow-memory/0.8.0-SNAPSHOT/arrow-memory-0.8.0-SNAPSHOT.jar:/Users/pcmoritz/.m2/repository/io/netty/netty-buffer/4.0.49.Final/netty-buffer-4.0.49.Final.jar:/Users/pcmoritz/.m2/repository/org/slf4j/slf4j-api/1.7.6/slf4j-api-1.7.6.jar:/Users/pcmoritz/.m2/repository/com/google/guava/guava/19.0/guava-19.0.jar:/Users/pcmoritz/.m2/repository/io/netty/netty-common/4.0.49.Final/netty-common-4.0.49.Final.jar:/Users/pcmoritz/.m2/repository/com/vlkan/flatbuffers/1.2.0-3f79e055/flatbuffers-1.2.0-3f79e055.jar:/Users/pcmoritz/.m2/repository/org/apache/arrow/arrow-format/0.8.0-SNAPSHOT/arrow-format-0.8.0-SNAPSHOT.jar:/Users/pcmoritz/.m2/repository/com/fasterxml/jackson/core/jackson-core/2.7.9/jackson-core-2.7.9.jar:/Users/pcmoritz/.m2/repository/com/fasterxml/jackson/core/jackson-databind/2.7.9/jackson-databind-2.7.9.jar:/Users/pcmoritz/.m2/repository/com/fasterxml/jackson/core/jackson-annotations/2.7.9/jackson-annotations-2.7.9.jar

RootAllocator allocator = new RootAllocator(1000000000);

ByteArrayInputStream in = new ByteArrayInputStream(Files.readAllBytes(Paths.get("/tmp/data.bin")));

in.skip(4) // Skip "number of tensors" field

ArrowStreamReader reader = new ArrowStreamReader(in, allocator);

reader.getVectorSchemaRoot().getSchema()

*/
