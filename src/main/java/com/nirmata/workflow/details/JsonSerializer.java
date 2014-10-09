package com.nirmata.workflow.details;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class JsonSerializer
{
    private static final Logger log = LoggerFactory.getLogger(JsonSerializer.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static ObjectNode newNode()
    {
        return mapper.createObjectNode();
    }

    public static ArrayNode newArrayNode()
    {
        return mapper.createArrayNode();
    }

    public static ObjectMapper getMapper()
    {
        return mapper;
    }

    public static byte[] toBytes(JsonNode node)
    {
        return nodeToString(node).getBytes();
    }

    public static String nodeToString(JsonNode node)
    {
        try
        {
            return mapper.writeValueAsString(node);
        }
        catch ( JsonProcessingException e )
        {
            log.error("mapper.writeValueAsString", e);
            throw new RuntimeException(e);
        }
    }

    public static JsonNode fromBytes(byte[] bytes)
    {
        return fromString(new String(bytes));
    }

    public static JsonNode fromString(String str)
    {
        try
        {
            return mapper.readTree(str);
        }
        catch ( IOException e )
        {
            log.error("reading JSON: " + str, e);
            throw new RuntimeException(e);
        }
    }

    private JsonSerializer()
    {
    }
}
