package com.github.chrisdoberman;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BankTransactionsProducerTest {

    @Test
    public void newRandomTransactionsTest() throws IOException {
        ProducerRecord<String, String> record = BankTransactionsProducer.newRandomTransaction("chris");
        String key = record.key();
        String value = record.value();

        assertEquals("chris", key);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.readTree(value);
        assertEquals("chris", node.get("name").asText());
        assertTrue("amount should be less than 100", node.get("amount").asInt() < 100);
    }
}
