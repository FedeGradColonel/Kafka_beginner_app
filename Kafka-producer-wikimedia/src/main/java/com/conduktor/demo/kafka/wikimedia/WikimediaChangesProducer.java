package com.conduktor.demo.kafka.wikimedia;

import com.conduktor.demo.kafka.wikimedia.handler.WikimediaChangeHandler;
import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@Slf4j
public class WikimediaChangesProducer {

    private final static String bootstrapServer = "localhost:9092";
    private final static String topic = "wikimedia-recentchange";
    private final static String wikimedia = "https://stream.wikimedia.org/v2/stream/recentchange";;

    public static void main(String[] args) throws InterruptedException {
        SpringApplication.run(WikimediaChangesProducer.class, args);

//        producer(getLocalhost());
        producer(getMyPlayground());
    }

    public static void producer(Properties properties) throws InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(wikimedia));
        EventSource eventSource = builder.build();

        eventSource.start();

        //Recupera dati per N secondi e poi si ferma
//        TimeUnit.SECONDS.sleep(30);
        TimeUnit.MINUTES.sleep(5);
        eventSource.close();
    }

    private static Properties getLocalhost(){
        Properties properties = new Properties();
        /* CONNECT TO LOCALHOST */
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        /* PRODUCER PROPERTIES */
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        /* PRODUCER PROPERTIES FOR KAFKA VERSION LESS THAN 2.8 */
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        /* PRODUCE PROPRERTIES FOR COMPRESSION AND BATCH */
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

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
        /* PRODUCE PROPRERTIES FOR COMPRESSION AND BATCH */
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        return properties;
    }

}