package com.conduktor.demo.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.Properties;

@SpringBootApplication
@Slf4j
public class KafkaBeginnerApplication {

	private static String bootstrapServer = "localhost:9092";

	@Autowired
	static
	KafkaTemplate kafkaTemplate;

	static final String TOPIC = "demo_java";

	public static void main(String[] args) {
		SpringApplication.run(KafkaBeginnerApplication.class, args);

//		ProducerRecord producerRecord = producerRecord(TOPIC, "uno", "record");
//		producer(getMemberPlayground(), producerRecord1);

		List<ProducerRecord> producerRecordList = List.of(
				new ProducerRecord(TOPIC, "id_1", "prova"),
				new ProducerRecord(TOPIC, "id_3", "cane"),
				new ProducerRecord(TOPIC, "id_1", "che"),
				new ProducerRecord(TOPIC, "id_3", "gatto"),
				new ProducerRecord(TOPIC, "id_1", "la configurazione"),
				new ProducerRecord(TOPIC, "id_2", "messaggio_kafka"),
				new ProducerRecord(TOPIC, "id_1", "funziona")
		);
		producer(getMemberPlayground(), producerRecordList);
//		producer(getLocalhost(), producerRecordList);
	}

	/*
	 * Producer con i parametri:
	 * key: String
	 * value: String
	 * e le properties settate precedentemente */
	public static void producer(Properties properties, List<ProducerRecord> producerRecordList){
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

		/* Invia dati a kafka */
//		producer.send(producerRecord);
//		log.info("dati inviati");

		for (int i = 0; i < 5; i++) {
			for (ProducerRecord producerRecord : producerRecordList) {
				/* invia i dati a Kafka
				 * viene eseguito ogni volta che un record viene inviato correttamente
				 * oppure quando avviene un'eccezione */
				producer.send(producerRecord, (RecordMetadata metadata, Exception e) -> {
					if (e == null) {
//						log.info("Received new metadata: " +
//								"\nTopic: " + metadata.topic() +
//								"\nPartition: " + metadata.partition() +
//								"\nOffset: " + metadata.offset() +
//								"\nKey: " + producerRecord.key() +
//								"\nValue: " + producerRecord.value() +
//								"\nTimestamp: " + metadata.timestamp());
						log.info("key: {}, value: {}", producerRecord.key(), producerRecord.partition());
					} else {
						log.error("Error while producing {} ", e);
					}
				});
			}
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		/* Dice al producer d'inviare tutti i dati e bloccarli fino al termine dell'operazione
		 * è un'operazione sincrona */
		producer.flush();
		log.info("operazione completata");

		/* Chiude il producer */
		producer.close();
		log.info("producer chiuso");
	}

	/*
	 * Record che viene passato al producer che lo invierà a kafka
	 * ha come parametri chiave e valore due stringe */
	public static ProducerRecord<String, String> producerRecord(String topicName, String key, String value){
		return new ProducerRecord<>(topicName, key, value);
	}

	private static Properties getLocalhost(){
		Properties properties = new Properties();
		/* CONNECT TO LOCALHOST */
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		/* PRODUCER PROPERTIES */
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400");
		return properties;
	}

	private static Properties getMyPlayground(){
		Properties properties = new Properties();
		/* CONNECT TO CONDUKTOR - MyPlayground */
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.playground.cdkt.io:9092");
		properties.setProperty("security.protocol", "SASL_SSL");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"2377JMgcpjfWxu40qROHjd\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIyMzc3Sk1nY3BqZld4dTQwcVJPSGpkIiwib3JnYW5pemF0aW9uSWQiOjY5MTc3LCJ1c2VySWQiOjc5ODg0LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI4NGVlMjViNC04YTM2LTRmN2ItYWYxMy00MjYzNGJiODk1ODAifX0.AhYVOxZfhw5zzIs5_yG6YNOdHuf0D0CBKMlh5yt9haY\";");
		properties.setProperty("sasl.mechanism", "PLAIN");
		/* PRODUCER PROPERTIES */
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400");
		return properties;
	}

	private static Properties getMemberPlayground(){
		Properties properties = new Properties();
		/* CONNECT TO CONDUKTOR - TeamPlayground */
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.playground.cdkt.io:9092");
		properties.setProperty("security.protocol", "SASL_SSL");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"corsokafka\" password=\"069e48ef-62a4-453a-b087-0b433a0a3aa8\";");
		properties.setProperty("sasl.mechanism", "PLAIN");
		/* PRODUCER PROPERTIES */
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400");
		return properties;
	}

}
