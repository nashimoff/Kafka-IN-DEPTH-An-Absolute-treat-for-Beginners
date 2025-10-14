package serialization;

import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> targetClass;

    @SuppressWarnings("unchecked")
	@Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String targetClassName = (String) configs.get("json.deserializer.class");
        try {
            if (targetClassName != null) {
                this.targetClass = (Class<T>) Class.forName(targetClassName);
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found in deserializer configuration", e);
        }
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, targetClass);
        } catch (Exception e) {
            System.err.println("Error deserializing message: " + e.getMessage());
            e.printStackTrace();
            return null; // or handle this case as appropriate for your use
        }
    }


    @Override
    public void close() {
        // Close resources if needed
    }
}
