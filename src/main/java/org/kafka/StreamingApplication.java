package org.kafka;

import avro.Fine;
import avro.UserFineDto;
import avro.UserInformation;
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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.jboss.logging.Logger;
import org.kafka.valuejoiner.UserInfoFineJoiner;

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
    private SpecificAvroSerde<UserFineDto> userFineDtoSpecificAvroSerde;



    @PostConstruct
    public void setup() throws IOException {
        userInformationSpecificAvroSerde = new SpecificAvroSerde<>();
        Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry:8085");
        userFineDtoSpecificAvroSerde = new SpecificAvroSerde<>();
        userFineDtoSpecificAvroSerde.configure(serdeConfig, false);
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
            kafkaProducer.send(new ProducerRecord<>("user-information", userInformation.get("ssn").toString(), userInformation));
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
            kafkaProducer.send(new ProducerRecord<>("fines", fine.get("id").toString(), fine));
        });
    }



    @Produces
    public Topology hello() {
        userInformationSpecificAvroSerde.configure(Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8080"), false);
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KeyValueBytesStoreSupplier userInfoStoreSupplier = Stores.persistentKeyValueStore("user-information-key-store");
        Materialized<String, UserInformation, KeyValueStore<Bytes, byte[]>> userInfoMaterialized = Materialized.<String, UserInformation>as(userInfoStoreSupplier)
                .withKeySerde(Serdes.String())
                .withValueSerde(userInformationSpecificAvroSerde);
        KTable<String, UserInformation> userInformationKStream = streamsBuilder.stream("user-information", Consumed.with(Serdes.String(), userInformationSpecificAvroSerde))
                .toTable(Named.as("user-information-table"), userInfoMaterialized);

        KeyValueBytesStoreSupplier storeSupplier = Stores.persistentKeyValueStore("fine-key-store");
        Materialized<String, List<Fine>, KeyValueStore<Bytes, byte[]>> materialized = Materialized.<String, List<Fine>>as(storeSupplier)
                        .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.ListSerde(ArrayList.class, fineSpecificAvroSerde));

        KTable<String, List<Fine>> fineKTable = streamsBuilder
                .stream("fines", Consumed.with(Serdes.String(), fineSpecificAvroSerde))
                .selectKey((key, fine) -> fine.get("ssn").toString())
                .groupByKey(Grouped.with(Serdes.String(), fineSpecificAvroSerde))
                .aggregate(
                        ArrayList::new,
                        (ssn, fine, fineList) -> {
                            fineList.add(fine);
                            return fineList;
                        },
                        materialized
                );


        KeyValueBytesStoreSupplier joinedStoreSupplier = Stores.persistentKeyValueStore("joined-key-store");
        Materialized<String, UserFineDto, KeyValueStore<Bytes, byte[]>> joinedMaterialized = Materialized.<String, UserFineDto>as(joinedStoreSupplier)
                .withKeySerde(Serdes.String())
                .withValueSerde(userFineDtoSpecificAvroSerde);

        KTable<String, UserFineDto> ssn = userInformationKStream
                .join(fineKTable,
                        new UserInfoFineJoiner(),
                        joinedMaterialized);
        ssn.toStream()
                .peek((key, value) -> System.out.println("$$$$$$$$$$$$$$$$   " + key + " : " + value + "$$$$$$$$$$$$$$$$$"))
                .to("user-fines-information", Produced.with(Serdes.String(), userFineDtoSpecificAvroSerde));

        return streamsBuilder.build();
    }

}
