package lilmonk.flink.tutorial.schema;

import com.google.gson.Gson;
import lilmonk.flink.tutorial.model.PageViewJson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class PageViewSchema implements DeserializationSchema<PageViewJson>, SerializationSchema<PageViewJson> {
    private static final Gson gson = new Gson();

    /**
     * Deserializes the byte message.
     *
     * @param message The message, as a byte array.
     * @return The deserialized message as an object (null if the message cannot be deserialized).
     */
    @Override
    public PageViewJson deserialize(byte[] message) throws IOException {
        return gson.fromJson(new String(message), PageViewJson.class);
    }

    /**
     * Method to decide whether the element signals the end of the stream. If true is returned the
     * element won't be emitted.
     *
     * @param nextElement The element to test for the end-of-stream signal.
     * @return True, if the element signals end of stream, false otherwise.
     */
    @Override
    public boolean isEndOfStream(PageViewJson nextElement) {
        return false;
    }

    /**
     * Serializes the incoming element to a specified type.
     *
     * @param pageView The incoming element to be serialized
     * @return The serialized element.
     */
    @Override
    public byte[] serialize(PageViewJson pageView) {
        return gson.toJson(pageView).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Gets the data type (as a {@link TypeInformation}) produced by this function or input format.
     *
     * @return The data type produced by this function or input format.
     */
    @Override
    public TypeInformation<PageViewJson> getProducedType() {
        return TypeInformation.of(PageViewJson.class);
    }
}
