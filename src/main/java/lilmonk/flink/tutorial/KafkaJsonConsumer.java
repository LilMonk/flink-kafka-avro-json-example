package lilmonk.flink.tutorial;

import lilmonk.flink.tutorial.model.PageViewJson;
import lilmonk.flink.tutorial.schema.PageViewSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaJsonConsumer {
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String TOPIC = "pageviews_json";
    private static final String CONSUMER_GRP = "consumer-grp-1";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DeserializationSchema<PageViewJson> deserializationSchema = new PageViewSchema();

        KafkaSource<PageViewJson> source = KafkaSource.<PageViewJson>builder()
                .setBootstrapServers(BOOTSTRAP_SERVER)
                .setTopics(TOPIC)
                .setGroupId(CONSUMER_GRP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(deserializationSchema))
                .build();

        DataStreamSource<PageViewJson> kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<String> pageViewStr = kafkaSource.map(
                (MapFunction<PageViewJson, String>) pageView ->
                        "PageId: " + pageView.getPageid() + "\t"
                                + "UserId: " + pageView.getUserid() + "\t"
                                + "ViewTime: " + pageView.getViewtime() + "\n");

        pageViewStr.print("Result");

        env.execute("Kafka avro consumer");
    }
}
