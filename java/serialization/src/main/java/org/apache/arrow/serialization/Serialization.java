import java.io.ByteArrayInputStream;

import org.apache.arrow.vector.stream.ArrowStreamReader;

import org.apache.arrow.memory.RootAllocator;

// /env -class-path /Users/pcmoritz/.m2/repository/org/apache/arrow/arrow-vector/0.8.0-SNAPSHOT/arrow-vector-0.8.0-SNAPSHOT.jar:/Users/pcmoritz/.m2/repository/org/apache/arrow/arrow-memory/0.8.0-SNAPSHOT/arrow-memory-0.8.0-SNAPSHOT.jar:/Users/pcmoritz/.m2/repository/io/netty/netty-buffer/4.0.49.Final/netty-buffer-4.0.49.Final.jar:/Users/pcmoritz/.m2/repository/org/slf4j/slf4j-api/1.7.6/slf4j-api-1.7.6.jar:/Users/pcmoritz/.m2/repository/com/google/guava/guava/19.0/guava-19.0.jar:/Users/pcmoritz/.m2/repository/io/netty/netty-common/4.0.49.Final/netty-common-4.0.49.Final.jar:/Users/pcmoritz/.m2/repository/com/vlkan/flatbuffers/1.2.0-3f79e055/flatbuffers-1.2.0-3f79e055.jar

RootAllocator allocator = new RootAllocator(1000000000);

ByteArrayInputStream in = new ByteArrayInputStream(Files.readAllBytes(Paths.get("/tmp/data.bin")));

ArrowStreamReader reader = new ArrowStreamReader(in, allocator);

reader.getVectorSchemaRoot().getSchema()
