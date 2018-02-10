package com.navercorp;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerTest {
	private static Logger LOGGER = LoggerFactory.getLogger("KafkaLogger");

	public static void main(String[] args) {

	}

	private void doTest(int threadSize) {

	}

	private class KafkaClient {
		private Consumer<String, String> consumer;

		private Callback logCallBack = new Callback() {
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (exception != null) {
					LOGGER.error("Kafka Send Error", exception);
					return;
				}

				LOGGER.info("{}", metadata.toString());
			}
		};

		KafkaClient(String topic) {
			Properties properties = new Properties();
			properties.put("bootstrap.server", "");
			properties.put("serializer.class", "kafka.serializer.StringEncoder");
			properties.put("auto.commit.interval.ms", "1000");

			this.consumer = new KafkaConsumer<String, String>(properties);
			this.consumer.subscribe(Arrays.asList(topic));
		}

		public void send(CustomModel model) {
			try {
				for (int i = 0; i < 100; i++) {
					ConsumerRecords<String, String> records = consumer.poll(1000);

					for (ConsumerRecord<String, String> record : records) {
						LOGGER.info("offset : {} / data : ", record.offset(), record.value());
					}
				}
			} finally {
				consumer.close();
			}
		}
	}

	private class CustomModel {
		private String key;
		private String message;

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public String getMessage() {
			return message;
		}

		public void setMessage(String message) {
			this.message = message;
		}
	}
}
