package com.conduktor.demo.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ConsumerMain {
    final static String GROUPID = "my-java-application";
    final static String TOPIC = "demo_java";

    public static void main(String[] args) {

        getRecord(consumer(getLocalhost()));
//        getRecord(consumer(getMyPlayground()));

    }

    public static void getRecord(KafkaConsumer consumer){
        /* RECUPERO DELLA REFERENZA DEL THREAD ATTUALMENTE IN USO */
        final Thread consumerThread = Thread.currentThread();
        /* AGGIUNTA DELLA CHIUSURA A GANCIO*/
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Rilevata una chiusura improvvisa, attivato il consumer.wakeup()");
            /* Lancia un'eccezione sul processo di recupero consumer.poll(..)*/
            consumer.wakeup();
            /* Unisce il consumerThread per permettere l'esecuzione del codice nel consumeThread*/
            try {
                consumerThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        try {
            /* RECUPERO DEI DATI */
            while (true) {
//                log.info("Recupero dati in corso...");
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
//                    log.info("Key: {}, value: {}", record.key(), record.value());
//                    log.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                }
            }
        } catch (WakeupException e){
            log.info("Il Consumer sta iniziando a chiudersi");
        } catch (Exception e){
            log.error("Eccezione inaspettata nel Consumer: {}", e);
        } finally {
            /* Chiude il Consumer ed esegue il commit degli offsets */
            consumer.close();
            log.info("Il Consumer è stato spento");
        }
    }

    public static KafkaConsumer<String, String> consumer(Properties properties) {
        /* Consumer con le properties passate come parametro */
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        /* Lista di topic */
        List<String> topicList = Arrays.asList(TOPIC);
        /* Prende in ingesso una lista di topic da gestire */
        consumer.subscribe(topicList);
        return consumer;
    }

    private static Properties getLocalhost(){
        Properties properties = new Properties();
        /* CONNECT TO LOCALHOST */
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        /* CONSUMER PROPERTIES */
        /* Deserializzazione dei dati */
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);
        /*
         * il parametro ha 3 configurazioni utilizzabili: "none/earliest/latest"
         * none: se non si ha un consumer-group allora andrà in errore, bisogna quindi impostare un Group prima di avviare l'applicazione
         * earliest: legge dall'inizio del topic
         * latest: legge solamente gli ultimi messaggi dall'avio dell'applicazione in poi
         */
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        return properties;
    }

    private static Properties getMyPlayground(){
        Properties properties = new Properties();
        /* CONNECT TO CONDUKTOR - MyPlayground */
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"2377JMgcpjfWxu40qROHjd\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIyMzc3Sk1nY3BqZld4dTQwcVJPSGpkIiwib3JnYW5pemF0aW9uSWQiOjY5MTc3LCJ1c2VySWQiOjc5ODg0LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI4NGVlMjViNC04YTM2LTRmN2ItYWYxMy00MjYzNGJiODk1ODAifX0.AhYVOxZfhw5zzIs5_yG6YNOdHuf0D0CBKMlh5yt9haY\";");
        properties.setProperty("sasl.mechanism", "PLAIN");
        /* CONSUMER PROPERTIES */
        /* Deserializzazione dei dati */
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    private static Properties getMemberPlayground(){
        Properties properties = new Properties();
        /* CONNECT TO CONDUKTOR - TeamPlayground */
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"corsokafka\" password=\"069e48ef-62a4-453a-b087-0b433a0a3aa8\";");
        properties.setProperty("sasl.mechanism", "PLAIN");
        /* CONSUMER PROPERTIES */
        /* Deserializzazione dei dati */
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}
