package com.navercorp;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerTest {
	private static Logger LOGGER = LoggerFactory.getLogger("KafkaLogger");

	public static void main(String[] args) {
		new ProducerTest().doTest();
	}

	private void doTest() {
		KafkaClient kafkaClient = new KafkaClient("test");

		for (int i = 0; i < 10000; i++) {
			CustomModel customModel = new CustomModel();
			customModel.setKey(Integer.toString((i % 20) + 1));
			customModel.setMessage("testMessage " + i);

			LOGGER.info("{ key : {}, message : {} }", customModel.getKey(), customModel.getMessage());

			kafkaClient.send(customModel);

			try {
				Thread.sleep(100);
			} catch (Exception e) {
				LOGGER.info("EXIT By Interrupted");
				return;
			}
		}
	}

	private class KafkaClient {
		private String topic;
		private Producer<String, String> producer;

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
			this.topic = topic;

			Properties properties = new Properties();
			properties.put("bootstrap.servers", "");
			properties.put("key.serializer", StringSerializer.class.getName());
			properties.put("value.serializer", StringSerializer.class.getName());

			this.producer = new KafkaProducer<String, String>(properties);
		}

		public void send(CustomModel model) {
			this.producer.send(new ProducerRecord<String, String>(topic, model.key, model.message), logCallBack);
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
