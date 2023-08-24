package org.kafka;

import avro.Fine;
import avro.UserInformation;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.*;

@ApplicationScoped
public class StreamingApplication {

    private static final String USER_INFORMATION_TOPIC = "user-information";
    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final Logger LOGGER = Logger.getLogger(StreamingApplication.class.getName());

    private SpecificAvroSerde<UserInformation> userInformationSpecificAvroSerde ;
    private SpecificAvroSerde<Fine> fineSpecificAvroSerde ;



    @PostConstruct
    public void setup() throws IOException {
        userInformationSpecificAvroSerde = new SpecificAvroSerde<>();
        fineSpecificAvroSerde = new SpecificAvroSerde<>();
        LOGGER.info("------------------------ Producer started -------------------");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, userInformationSpecificAvroSerde.serializer());
        produceUserInformationMessages(properties);
        produceFineMessages(properties);
    }

    private void produceUserInformationMessages(Properties properties) throws IOException {
        Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry:8085");
        userInformationSpecificAvroSerde.configure(serdeConfig, false);
        UserInformation sumanth = new UserInformation("Sumanth", "101", "sumanth.rajesh@anywhere.com", "10/10/1998");
        UserInformation raghu = new UserInformation("Raghu", "102", "raghu@anywhere.com", "11/10/1998");
        UserInformation praveen = new UserInformation("Praveen", "103", "praveen@anywhere.com", "12/10/1998");
        UserInformation sujay = new UserInformation("Sujay", "104", "sujay@anywhere.com", "13/10/1998");
        List<UserInformation> userInformationList = List.of(sumanth, raghu, praveen, sujay);
        KafkaProducer<String, UserInformation> kafkaProducer = new KafkaProducer<>(properties, Serdes.String().serializer(), userInformationSpecificAvroSerde.serializer());
        userInformationList.forEach((UserInformation userInformation) -> {
            LOGGER.info("-------------------------- Traversing list ------------------");
            kafkaProducer.send(new ProducerRecord<>("user-information", userInformation.get("ssn").toString(), userInformation), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e != null){
                        LOGGER.error(e.getMessage());
                    } else{
                        LOGGER.info("Message sent successfully");
                    }
                }
            });
            kafkaProducer.flush();
        });
    }
    private void produceFineMessages(Properties properties) throws IOException {
        Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry:8085");
        fineSpecificAvroSerde.configure(serdeConfig, false);
        List<Fine> fineList = List.of(
                new Fine("1", "RED_LIGHT_VIOLATION", "100", "15/08/2023", "101"),
                new Fine("2", "DRINK_AND_DRIVE", "150", "16/08/2023", "101"),
                new Fine("3", "OVER_SPEEDING", "200", "17/08/2023", "102"),
                new Fine("4", "ONE_WAY_VIOLATION", "250", "18/08/2023", "102"),
                new Fine("5", "NO_SEAT_BELT", "300", "19/08/2023", "102"),
                new Fine("6", "RED_LIGHT_VIOLATION", "100", "20/08/2023", "103"),
                new Fine("7", "NO_SEAT_BELT", "300", "21/08/2023", "103"),
                new Fine("8", "OVER_SPEEDING", "200", "22/08/2023", "103"),
                new Fine("9", "DRINK_AND_DRIVE", "150", "23/08/2023", "104"),
                new Fine("10", "ONE_WAY_VIOLATION", "250", "24/08/2023", "104")
        );
        KafkaProducer<String, Fine> kafkaProducer = new KafkaProducer<>(properties, Serdes.String().serializer(), fineSpecificAvroSerde.serializer());
        fineList.forEach((Fine fine) -> {
            LOGGER.info("-------------------------- Traversing list ------------------");
            kafkaProducer.send(new ProducerRecord<>("fines", fine.get("id").toString(), fine), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e != null){
                        LOGGER.error(e.getMessage());
                    } else{
                        LOGGER.info("Message sent successfully");
                    }
                }
            });
        });
    }

    private void printSuccessMessage(){
        LOGGER.info("Message send success!!");
    }


    @Produces
    public Topology hello() {
        userInformationSpecificAvroSerde.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8080"), false);
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream("user-information", Consumed.with(Serdes.String(), userInformationSpecificAvroSerde))
                .peek((key, value) -> System.out.println("Hello : " + key + ":" + value));
        streamsBuilder.stream("fines", Consumed.with(Serdes.String(), fineSpecificAvroSerde))
                .peek((key, value) -> System.out.println("Hello : " + key + ":" + value));


        Topology topology = streamsBuilder.build();
        return topology;
    }

}
