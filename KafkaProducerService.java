import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.kafka.clients.producer.*;

import com.demo.kafka.consumer.*;
import com.fasterxml.jackson.databind.*;

public class KafkaProducerService {

	public static void main(String[] args) {
		String topic = "stockData";
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");

		ObjectMapper objectMapper = new ObjectMapper();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		Producer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(props);
		Stock details = null;

		for (int i = 0; i < 10000000; i++) {
			String timestamp = dateFormat.format(new Date());
			System.out.println("timestamp is : " + timestamp);
			// PriceData takes argumens in the order close, high, low, open, volume
			details = new Stock("BTC", timestamp,
					new PriceData(8000 + gen(100), 8100 + gen(200), 7900 + gen(100), 8000 + gen(100), gen(10000)));
			send(topic, objectMapper, producer, details);

			details = new Stock("ETH", timestamp,
					new PriceData(200 + gen(10), 210 + gen(10), 190 + gen(10), 200 + gen(10), gen(1000)));
			send(topic, objectMapper, producer, details);

			details = new Stock("LTC", timestamp,
					new PriceData(100 + gen(10), 110 + gen(10), 90 + gen(10), 100 + gen(10), gen(400)));
			send(topic, objectMapper, producer, details);

			details = new Stock("XRP", timestamp,
					new PriceData(50 + gen(5), 55 + gen(5), 45 + gen(5), 50 + gen(5), gen(300)));
			send(topic, objectMapper, producer, details);
			try {
				Thread.sleep(60 * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private static void send(String topic, ObjectMapper objectMapper, Producer<String, JsonNode> producer,
			Stock details) {
		producer.send(new ProducerRecord<String, JsonNode>(topic, objectMapper.valueToTree(details)));
	}

	static float gen(int n) {
		return ThreadLocalRandom.current().nextLong(n);
	}
}
