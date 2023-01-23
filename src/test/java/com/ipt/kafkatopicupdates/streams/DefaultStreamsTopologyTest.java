
package com.ipt.kafkatopicupdates.streams;

import ch.ipt.kafka.avro.Authorization;
import com.ipt.kafkatopicupdates.StreamsUtils;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;

import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;



class DefaultStreamsTopologyTest {

    private DefaultStreamsTopology defaultStreamsTopology;

    private Properties config;
    private Map<String, Object> serdeConfig;
    private TopologyTestDriver testDriver;

    private String sourceTopic = "authorizations-with-duplicates";
    private String sinkTopic = "authorizations-without-duplicates";
    private String STORE_NAME = "test-store";

    private TestInputTopic<String, Authorization> inputTopic;
    private TestOutputTopic<String, Authorization> outputTopic;

    private final Serde<String> stringSerde = Serdes.String();
    private SpecificAvroSerde<Authorization> authorizationSerde;



    @BeforeEach
    public void setUp() {
        config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");

        config.put("schema.registry.url", "mock://test");

        serdeConfig = StreamsUtils.propertiesToMap(config);
        authorizationSerde = StreamsUtils.getSpecificAvroSerde(serdeConfig);

        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        defaultStreamsTopology = new DefaultStreamsTopology();
        defaultStreamsTopology.buildTopology(streamsBuilder, sourceTopic, sinkTopic, STORE_NAME);

        Topology topology = streamsBuilder.build();

        testDriver = new TopologyTestDriver(topology, config);

        inputTopic = testDriver.createInputTopic(sourceTopic, stringSerde.serializer(), authorizationSerde.serializer());
        outputTopic = testDriver.createOutputTopic(sinkTopic, stringSerde.deserializer(), authorizationSerde.deserializer());
    }

    //create afterEach method
    @AfterEach
    public void tearDown() {
        testDriver.close();

        config = null;
        serdeConfig = null;
        authorizationSerde = null;
        defaultStreamsTopology = null;
        inputTopic = null;
        outputTopic = null;
    }


    @Test
    void firstRecordEntry() {
        // ARRANGE
        String accountID = "1";
        boolean authorized = true;
        Authorization firstRecord = createRecord(accountID, authorized);

        // ACT
        inputTopic.pipeInput(accountID, firstRecord);

        // ASSERT
        assertThat(
                outputTopic.isEmpty(),
                equalTo(false)
        );
        assertThat(
                outputTopic.readValue().getAuthorized(),
                equalTo(firstRecord.getAuthorized())
        );
        assertThat(
                outputTopic.isEmpty(),
                equalTo(true)
        );
        // get the state store and first record
        KeyValueStore<String, String> store = testDriver.getKeyValueStore(STORE_NAME);
        String storedRecord = store.get(firstRecord.getAccountId().toString());
        assertThat(
                storedRecord,
                equalTo(
                        String.valueOf(firstRecord.getAuthorized())
                )
        );
    }

    @Test
    void multipleDuplicateRecords() {
        // ARRANGE
        String accountID = "1";
        boolean authorized = true;
        Authorization firstRecord = createRecord(accountID, authorized);
        Authorization secondRecord = createRecord(accountID, authorized);
        Authorization thirdRecord = createRecord(accountID, authorized);

        // ACT
        inputTopic.pipeInput(accountID, firstRecord);
        // ASSERT
        assertThat(
                outputTopic.isEmpty(),
                equalTo(false)
        );
        assertThat(
                outputTopic.readValue().getAuthorized(),
                equalTo(firstRecord.getAuthorized())
        );

        //ACT
        inputTopic.pipeInput(accountID, secondRecord);
        //ASSERT
        assertThat(
                outputTopic.isEmpty(),
                equalTo(true)
        );

        inputTopic.pipeInput(accountID, thirdRecord);
        //ASSERT
        assertThat(
                outputTopic.isEmpty(),
                equalTo(true)
        );

        // get the state store and ensure that the last record state is stored
        KeyValueStore<String, String> store = testDriver.getKeyValueStore(STORE_NAME);
        String storedRecord = store.get(firstRecord.getAccountId().toString());
        assertThat(
                storedRecord,
                equalTo(
                        String.valueOf(firstRecord.getAuthorized())
                )
        );
    }

    @Test
    void stateSwitchFromTrueToFalse() {
        // ARRANGE
        String accountID = "1";
        boolean authorized = true;
        Authorization firstRecord = createRecord(accountID, authorized);
        Authorization secondRecord = createRecord(accountID, !authorized);
        KeyValueStore<String, String> store = testDriver.getKeyValueStore(STORE_NAME);


        // ACT
        inputTopic.pipeInput(accountID, firstRecord);
        // ASSERT
        assertThat(
                outputTopic.isEmpty(),
                equalTo(false)
        );
        assertThat(
                outputTopic.readValue().getAuthorized(),
                equalTo(firstRecord.getAuthorized())
        );
        String storedRecord = store.get(firstRecord.getAccountId().toString());
        assertThat(
                storedRecord,
                equalTo(
                        String.valueOf(firstRecord.getAuthorized())
                )
        );

        //ACT
        inputTopic.pipeInput(accountID, secondRecord);
        //ASSERT
        assertThat(
                outputTopic.isEmpty(),
                equalTo(false)
        );
        assertThat(
                outputTopic.readValue().getAuthorized(),
                equalTo(secondRecord.getAuthorized())
        );

        // get the state store and ensure that the last record state is stored
        storedRecord = store.get(firstRecord.getAccountId().toString());
        assertThat(
                storedRecord,
                equalTo(
                        String.valueOf(secondRecord.getAuthorized())
                )
        );
    }

    @Test
    void stateSwitchFromFalseToTrue() {
        // ARRANGE
        String accountID = "1";
        boolean authorized = false;
        Authorization firstRecord = createRecord(accountID, authorized);
        Authorization secondRecord = createRecord(accountID, !authorized);
        KeyValueStore<String, String> store = testDriver.getKeyValueStore(STORE_NAME);

        // ACT
        inputTopic.pipeInput(accountID, firstRecord);
        // ASSERT
        assertThat(
                outputTopic.isEmpty(),
                equalTo(false)
        );
        assertThat(
                outputTopic.readValue().getAuthorized(),
                equalTo(firstRecord.getAuthorized())
        );
        String storedRecord = store.get(firstRecord.getAccountId().toString());
        assertThat(
                storedRecord,
                equalTo(
                        String.valueOf(firstRecord.getAuthorized())
                )
        );

        //ACT
        inputTopic.pipeInput(accountID, secondRecord);
        //ASSERT
        assertThat(
                outputTopic.isEmpty(),
                equalTo(false)
        );
        assertThat(
                outputTopic.readValue().getAuthorized(),
                equalTo(secondRecord.getAuthorized())
        );

        // get the state store and ensure that the last record state is stored
        storedRecord = store.get(firstRecord.getAccountId().toString());
        assertThat(
                storedRecord,
                equalTo(
                        String.valueOf(secondRecord.getAuthorized())
                )
        );
    }

    private Authorization createRecord(String id, boolean isAuthorized) {
        return new Authorization(id, isAuthorized);
    }
}
