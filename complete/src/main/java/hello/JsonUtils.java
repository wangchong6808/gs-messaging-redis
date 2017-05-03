package hello;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JsonUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String marshal(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static <T> T unmarshal(String jsonString, Class<T> type) {
        try {
            return objectMapper.readValue(jsonString, type);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static <T> T unmarshal(String jsonString, TypeReference<T> type) {
        try {
            return objectMapper.readValue(jsonString, type);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
