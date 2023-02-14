package com.conduktor.demo.kafka.openseach;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.http.ssl.SSLContexts;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.opensearch.client.RestHighLevelClient;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@SpringBootApplication
@Slf4j
public class OpenSearchKafkaConsumer {

    private final static String GROUPID = "dioporco";
    private final static String TOPIC = "wikimedia-recentchange";

    private final static String INDEX = "wikimedia";

    private static final String HTTP_LOCALHOST_9200 = "http://localhost:9200";
    //capire che tipo di file necessita SSL
    private static final String PATH_FILE_SSL = "/path/to/certificate.jks";


    public static void main(String[] args) throws IOException {
        SpringApplication.run(OpenSearchKafkaConsumer.class, args);
        RestHighLevelClient openSearchClient = createOpenSearchClient(HTTP_LOCALHOST_9200);

        KafkaConsumer<String, String> consumer = createKafkaConsumer(getMyPlayground(), TOPIC);

        sendRecords(openSearchClient, consumer, INDEX);

    }

    public static KafkaConsumer<String, String> createKafkaConsumer(Properties properties, String topic) {
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton(topic));
        return consumer;

    }

    public static void sendRecords(RestHighLevelClient openSearchClient, KafkaConsumer consumer, String index) throws IOException {
        try (openSearchClient; consumer) {

            // ottiene la referenza del Thread principale
            final Thread mainThread = Thread.currentThread();
            // aggiunge la chiusura a uncino
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Rilevata una chiusura improvvisa, attivato il consumer.wakeup()");
                /* Lancia un'eccezione sul processo di recupero consumer.poll(..)*/
                consumer.wakeup();
                /* Unisce il consumerThread per permettere l'esecuzione del codice nel consumeThread*/
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }));

            boolean indexExists = openSearchClient.indices()
                    .exists(new GetIndexRequest(index), RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info(index + " index has been created");
            } else {
                log.info(index + " index already exists");
            }

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Ricevuto {} record(s)", recordCount);

                BulkRequest bulkRequest = new BulkRequest();

                // send records into OpenSearch
                for (ConsumerRecord<String, String> record : records) {

                    //strategia 1
                    //definire un id utilizzando le coordinante del record
//                    String id = record.topic() + " " + record.partition() + " " + record.offset();

                    try {
                        //strategia 2
                        // estrarre un id da un JSON
                        String id = extractId(record.value());

                        IndexRequest indexRequest = new IndexRequest(index)
                                .source(record.value(), XContentType.JSON)
                                .id(id);

                        bulkRequest.add(indexRequest);
//                        IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
//                        log.info("Insert: {}, into OpenSearch", response.getId());
                    } catch (Exception e) {
                        log.error("Error: {}", e.getMessage());
                    }
                }

                if(bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserito {} record(s)", bulkResponse.getItems().length);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e){
                        log.error("Error: {}", e);
                    }
                    // IMPOSTAZIONE DEL COMMIT DELL'OFFSET AUTO DISABILITATA
                    // in questo modo si imposta manualmente il commit
                    // commit offset dopo che il Batch è stato elaborato
                    // TODO COMMENTARE PER AUTO COMMIT
                    consumer.commitSync();
                    log.info("l'Offset è stato committato");
                }
            }
        } catch (WakeupException e){
            log.info("Il Consumer sta iniziando a chiudersi");
        } catch (Exception e){
            log.error("Eccezione inaspettata nel Consumer: {}", e);
        } finally {
            /* Chiude il Consumer ed esegue il commit degli offsets */
            consumer.close();
            openSearchClient.close();
            log.info("Il Consumer è stato spento");
        }
    }

    private static String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }


    public static RestHighLevelClient createOpenSearchClient(String connection) {
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connection);
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            restHighLevelClient = new RestHighLevelClient(RestClient.builder((
                    new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))));
        } else {
            String[] auth = userInfo.split(":");

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
//                                            .setSSLContext(getSSLContext(pathFileSSL))
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
        }
        return restHighLevelClient;
    }

    private static SSLContext getSSLContext(String path) {
        try {
            // Carica il certificato SSL
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            FileInputStream inputStream = new FileInputStream(new File(path));
            try {
                trustStore.load(inputStream, "certificatePassword".toCharArray());
            } finally {
                inputStream.close();
            }
            // Inizializza il contesto SSL
            SSLContext sslContext = SSLContexts.custom()
                    .loadTrustMaterial(trustStore, null)
                    .build();

            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize the SSLContext", e);
        }
    }

    private static Properties getLocalhost() {
        Properties properties = new Properties();
        /* CONNECT TO LOCALHOST */
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        /* CONSUMER PROPERTIES */
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        // TODO COMMENTARE PER AUTO COMMIT
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return properties;
    }

    private static Properties getMyPlayground() {
        Properties properties = new Properties();
        /* CONNECT TO CONDUKTOR - MyPlayground */
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"2377JMgcpjfWxu40qROHjd\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIyMzc3Sk1nY3BqZld4dTQwcVJPSGpkIiwib3JnYW5pemF0aW9uSWQiOjY5MTc3LCJ1c2VySWQiOjc5ODg0LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI4NGVlMjViNC04YTM2LTRmN2ItYWYxMy00MjYzNGJiODk1ODAifX0.AhYVOxZfhw5zzIs5_yG6YNOdHuf0D0CBKMlh5yt9haY\";");
        properties.setProperty("sasl.mechanism", "PLAIN");
        /* CONSUMER PROPERTIES */
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUPID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        // TODO COMMENTARE PER AUTO COMMIT
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return properties;
    }

}