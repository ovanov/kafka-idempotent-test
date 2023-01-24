
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;



class AuthorizationDeduplicationTopologyTest {

    private AuthorizationDeduplicationTopology authorizationDeduplicationTopology;

    private final Properties config =
            StreamsUtils.getPropertiesConfig("test", "mock://test");
    private final Map<String, Object> serdeConfig = StreamsUtils.propertiesToMap(config);
    private final SpecificAvroSerde<Authorization> authorizationSerde =
            StreamsUtils.getSpecificAvroSerde(serdeConfig);
    private final Serde<String> stringSerde = Serdes.String();
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Authorization> inputTopic;
    private TestOutputTopic<String, Authorization> outputTopic;

    private String sourceTopic = "authorizations-with-duplicates";
    private String sinkTopic = "authorizations-without-duplicates";
    private String STORE_NAME = "test-store";


    @BeforeEach
    public void setUp() {

        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        authorizationDeduplicationTopology = new AuthorizationDeduplicationTopology();
        authorizationDeduplicationTopology.buildTopology(streamsBuilder, sourceTopic, sinkTopic, STORE_NAME);

        Topology topology = streamsBuilder.build();

        testDriver = new TopologyTestDriver(topology, config);

        inputTopic = testDriver.createInputTopic(sourceTopic, stringSerde.serializer(), authorizationSerde.serializer());
        outputTopic = testDriver.createOutputTopic(sinkTopic, stringSerde.deserializer(), authorizationSerde.deserializer());
    }

    //create afterEach method
    @AfterEach
    public void tearDown() {
        testDriver.close();

        authorizationDeduplicationTopology = null;
        inputTopic = null;
        outputTopic = null;
    }


    @Test
    @DisplayName("Test behaviour when the first record is written to a topic.")
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
    @DisplayName("Test behaviour when multiple identical records are written to a topic. (Duplicates)")

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
    @DisplayName("Test behaviour, weather a state switch from `True` to `False` is processed correctly.")
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
    @DisplayName("Test behaviour, weather a state switch from `False` to `True` is processed correctly.")
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
