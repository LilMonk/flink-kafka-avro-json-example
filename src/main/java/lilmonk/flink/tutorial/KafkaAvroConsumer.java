package lilmonk.flink.tutorial;

import lilmonk.flink.tutorial.model.PageView;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaAvroConsumer {
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String SCHEMA_REGISTRY = "http://localhost:8081";
    private static final String TOPIC = "pageviews";
    private static final String CONSUMER_GRP = "consumer-grp-1";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DeserializationSchema<PageView> deserializationSchema = ConfluentRegistryAvroDeserializationSchema.forSpecific(PageView.class, SCHEMA_REGISTRY);
        KafkaSource<PageView> source = KafkaSource.<PageView>builder()
                .setBootstrapServers(BOOTSTRAP_SERVER)
                .setTopics(TOPIC)
                .setGroupId(CONSUMER_GRP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(deserializationSchema))
                .build();

        DataStreamSource<PageView> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<String> pageViewStr = kafkaSource.map(
                (MapFunction<PageView, String>) pageView ->
                        "PageId: " + pageView.getPageid() + "\t"
                                + "UserId: " + pageView.getUserid() + "\t"
                                + "ViewTime: " + pageView.getViewtime() + "\n");

        pageViewStr.print("Result");

        env.execute("Kafka avro consumer");
    }
}
