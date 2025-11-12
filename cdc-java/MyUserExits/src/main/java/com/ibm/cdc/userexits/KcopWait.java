/* _______________________________________________________ {COPYRIGHT-TOP} _____
 * IBM Confidential
 * IBM InfoSphere Data Replication Source Materials
 *
 * 5725-E30 IBM InfoSphere Data Replication
 * 5725-E30 IBM InfoSphere Data Replication for Database Migration
 *
 * 5724-U70 IBM InfoSphere Change Data Delivery
 * 5724-U70 IBM InfoSphere Change Data Delivery for PureData System for Analytics
 * 5724-Q36 IBM InfoSphere Change Data Delivery for Information Server
 * 5724-Q36 IBM InfoSphere Change Data Delivery for PureData System for Analytics
 * for Information Server
 *
 * (C) Copyright IBM Corp. 2017  All Rights Reserved.
 *
 * The source code for this program is not published or otherwise
 * divested of its trade secrets, irrespective of what has been
 * deposited with the U.S. Copyright Office.
 * _______________________________________________________ {COPYRIGHT-END} _____*/

/****************************************************************************
** The following sample of source code ("Sample") is owned by International 
** Business Machines Corporation or one of its subsidiaries ("IBM") and is 
** copyrighted and licensed, not sold. You may use, copy, modify, and 
** distribute the Sample in any form without payment to IBM.
** 
** The Sample code is provided to you on an "AS IS" basis, without warranty of 
** any kind. IBM HEREBY EXPRESSLY DISCLAIMS ALL WARRANTIES, EITHER EXPRESS OR 
** IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF 
** MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. Some jurisdictions do 
** not allow for the exclusion or limitation of implied warranties, so the above 
** limitations or exclusions may not apply to you. IBM shall not be liable for 
** any damages you suffer as a result of using, copying, modifying or 
** distributing the Sample, even if IBM has been advised of the possibility of 
** such damages.
*****************************************************************************/

package com.ibm.cdc.userexits;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import com.datamirror.ts.target.publication.userexit.ReplicationEventTypes;
import com.datamirror.ts.target.publication.userexit.UserExitException;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaKcopOperationInIF;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaKcopReplicationCoordinatorIF;
import com.datamirror.ts.target.publication.userexit.sample.kafka.*;

/**
 * 
 * <p>
 * This Kcop code produces the same behavior as the default Kafka Replication
 * code, ie. the behavior without the
 * Kafka Custom Operation Processor, KCOP, ( aka User Exit). As such, this code
 * makes use of the Confluent
 * serializer, thus transparently registering schemas. Because the default
 * Confluent serializer
 * requires a schema registry url and this code is responsible for serializing,
 * the schema registry url
 * must be passed into this KCOP as a parameter.
 * </p>
 * 
 * <p>
 * This Sample should provide the foundation for implementing slight tweaks to
 * existing behavior if desired.
 * </p>
 * 
 * <p>
 * NOTE 1: The createProducerRecords class is not thread safe. However a means
 * is provided so that each thread
 * can store its own copy of non-theadsafe objects. Please see how this is done
 * below.
 * </p>
 * 
 * <p>
 * NOTE 2: The records returned by createProducerRecords are not deep copied, so
 * each call to the method should
 * generate new records and not attempt to keep references to old ones for
 * reuse.
 * </p>
 * 
 * <p>
 * Note 3: The KCOP is instantiated once per subscription which registers the
 * KCOP. This means if statics are made
 * use of, they will potentially be shared across the instantiated KCOPs
 * belonging to multiple actively replicating
 * subscriptions.
 * </p>
 * 
 */
public class KcopWait extends AbstractAvroKcop {

    /**
     * <p>
     * createProducerRecords is called once per subscribed operation that occurs on
     * the CDC Source. This method is called by multiple threads
     * and as such operations are not necessarily processed in order. It is valid to
     * return no producer records if a user wishes nothing to
     * be written to User Kafka Topics in response to the operation being processed.
     * </p>
     * 
     * <p>
     * For a given replication session, Kafka records written to the same topic and
     * same partition will be written to Kafka in order.
     * ie. Although formatted out of order, for a given topic/partition combination
     * the resultant records will be sent in the original order
     * for a replication session.
     * </p>
     * 
     * <p>
     * By virtue of being able to specify the producer records, a user can
     * determine:
     * </p>
     * 
     * <p>
     * 1) the topic(s) written to in response to an operation. You could potentially
     * alter the default behavior to write to two different topics.<br>
     * 2) The partition of a topic the kafka record is written to. This KCOP always
     * writes to partition 0 of a topic as per the default behavior.<br>
     * 3) The format and bytes of the Key and Value portions of the Kafka record.
     * eg. The user has control over serialization
     * </p>
     * 
     * <p>
     * In contrast to the kafkaKcopCoordinator, the kafkaKcopOperationIn holds
     * information relevant to the specific operation being processed and can thus
     * be thought of as specific operation scope rather than subscription scope.
     * </p>
     *
     * <p>
     * NOTE 1: The createProducerRecords class is not thread safe. However a means
     * is provided so that each thread
     * can store its own copy of non-theadsafe objects. Please see how this is done
     * below.
     * </p>
     * 
     * <p>
     * NOTE 2: The records returned by createProducerRecords are not deep copied, so
     * each call to the method should
     * generate new records and not attempt to keep references to old ones for
     * reuse.
     * </p>
     * 
     * @param kafkaKcopOperationIn Contains information relevant to the current
     *                             operation being processed.
     * @param kafkaKcopCoordinator Contains subscription scope information including
     *                             storage for each thread's non-threadsafe objects.
     * @return array of Kafka producer records
     * @throws UserExitException any error
     */
    @Override
    public ArrayList<ProducerRecord<byte[], byte[]>> createProducerRecords(KafkaKcopOperationInIF kafkaKcopOperationIn,
            KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException {
        // throw an exception if a table does not contain a key column
        GenericRecord kafkaKeyRecord = kafkaKcopOperationIn.getKafkaAvroKeyGenericRecord();
        if (kafkaKeyRecord == null) {
            handleNullKeyRecord(kafkaKcopOperationIn);
        }
        // The list of records to be applied to user Kafka topics in response to the
        // original source database operation.
        // The order of operations in the list determines their relative order in being
        // applied for a replication session, If
        // the records refer to the same topic/partition combination.
        ArrayList<ProducerRecord<byte[], byte[]>> producerRecordsToReturn = new ArrayList<>();

        // Retrieve the thread specific user defined object.
        @SuppressWarnings("unchecked")
        ThreadSpecificContext<PersistentSerializerObject> threadSpecificContext = (ThreadSpecificContext<PersistentSerializerObject>) kafkaKcopCoordinator
                .getKcopThreadSpecificContext();

        // Wait for a random time between 5 and 10 seconds
        java.util.Random random = new java.util.Random();
        int delay = 5000 + random.nextInt(5000); // 5000ms (5s) to 10000ms (10s)
        try {
            kafkaKcopCoordinator.logEvent("Waiting for " + (delay / 1000) + " seconds...");
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Handle interruption as appropriate for your context
        }

        if (threadSpecificContext == null) {
            // No thread specific user defined object existed for this thread, create one so
            // we can place our non-threadsafe objects
            // in it. We will now not need to re-instantiate objects on each call to
            // createProducerRecords, rather we can reuse ones
            // stored here.
            threadSpecificContext = createThreadSpecificContext(createPersistentSerializerObject());
            // Having created an object for this thread which itself contains useful
            // objects, some of which may be non-threadsafe,
            // store the object, so we won't need to recreate the next time this thread
            // calls createProducerRecords.
            kafkaKcopCoordinator.setKcopThreadSpecificContext(threadSpecificContext);
        }

        // Process the various events we have subscribed to, generating appropriate
        // Kafka ProducerRecords as a result.
        switch (kafkaKcopOperationIn.getReplicationEventType()) {
            case ReplicationEventTypes.BEFORE_INSERT_EVENT ->                 {
                    ProducerRecord<byte[], byte[]> insertKafkaAvroProducerRecord = createDefaultAvroBinaryInsertProducerRecord(
                            kafkaKcopCoordinator,
                            kafkaKcopOperationIn,
                            threadSpecificContext);
                    // An insert on the source database is represented by one resultant Kafka
                    // ProducerRecord in the appropriate Kafka topic.
                    producerRecordsToReturn.add(insertKafkaAvroProducerRecord);
                }
            case ReplicationEventTypes.BEFORE_DELETE_EVENT -> // A delete on the source database is represented by one resultant Kafka
                // ProducerRecord in the appropriate Kafka topic.
                // The key of the delete indicates which row no longer exists, the value bytes
                // of a delete's Kafka ProducerRecord is null.
                producerRecordsToReturn.add(createDefaultAvroBinaryDeleteProducerRecord(
                        kafkaKcopCoordinator,
                        kafkaKcopOperationIn,
                        threadSpecificContext,
                        genericRecordRebuilder.rebuild(kafkaKcopOperationIn.getKafkaAvroKeyGenericRecord())));
            case ReplicationEventTypes.BEFORE_UPDATE_EVENT ->                 {
                    // Determine if this update altered one of the Kafka Key Columns
                    if (kafkaKcopOperationIn.getKafkaAvroUpdatedKeyGenericRecord() != null) {
                        // Because the key has changed we need to first delete the kafka record with the
                        // old key and then insert the new one.
                        producerRecordsToReturn.add(createDefaultAvroBinaryDeleteProducerRecord(
                                kafkaKcopCoordinator,
                                kafkaKcopOperationIn,
                                threadSpecificContext,
                                genericRecordRebuilder.rebuild(kafkaKcopOperationIn.getKafkaAvroUpdatedKeyGenericRecord())));
                        
                    }       // In both the key update case and the non-key update case an insert of the new
                    // record being updated to is required.
                    ProducerRecord<byte[], byte[]> insertKafkaAvroProducerRecord = createDefaultAvroBinaryInsertProducerRecord(
                            kafkaKcopCoordinator,
                            kafkaKcopOperationIn,
                            threadSpecificContext);
                    producerRecordsToReturn.add(insertKafkaAvroProducerRecord);
                }
            default -> {
            }
        }
        // Note that their are two records being returned in response to an update which
        // affected a key column. The delete is added
        // to the List first, followed by the insert so that this is the expected order
        // they will be seen on the User Kafka topic.
        return producerRecordsToReturn;
    }

    /**
     * 
     * @param kafkaKcopOperationIn  - Contains information relevant to the current
     *                              operation being processed.
     * @param threadSpecificContext - Contains subscription scope information
     *                              including storage for each thread's
     *                              non-threadsafe objects, in this case our
     *                              serializers.
     * @return A single Kafka Producer Record representing an insert on the source
     *         table to be written to a User Kafka Topic.
     * @throws UserExitException
     */
    protected ProducerRecord<byte[], byte[]> createDefaultAvroBinaryInsertProducerRecord(
            KafkaKcopReplicationCoordinatorIF kafkaKcopReplicationCoordinator,
            KafkaKcopOperationInIF kafkaKcopOperationIn,
            ThreadSpecificContext<PersistentSerializerObject> threadSpecificContext) throws UserExitException {

        ProducerRecord<byte[], byte[]> insertKafkaAvroProducerRecord;

        // Wait for a random time between 5 and 10 seconds
        java.util.Random random = new java.util.Random();
        int delay = 5000 + random.nextInt(5000); // 5000ms (5s) to 10000ms (10s)
        try {
            kafkaKcopReplicationCoordinator.logEvent("Waiting for " + (delay / 1000) + " seconds...");
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Handle interruption as appropriate for your context
        }

        // For the Insert generate the bytes to place in the ProducerRecord for the Key
        // field. This handles a case where the key is null, which is not something
        // expected for default Kafka Replication, but we'll add as this code for
        // completeness. Note that the serializer employed here is registering the Avro
        // Generic Data Record with the Confluent schema registry transparently.
        String topic = threadSpecificContext.remap(kafkaKcopOperationIn.getKafkaTopicName());
        GenericRecord kafkaAvroKeyGenericRecord = genericRecordRebuilder
                .rebuild(kafkaKcopOperationIn.getKafkaAvroKeyGenericRecord());
        byte[] kafkaAvroKeyByteArray = kafkaAvroKeyGenericRecord == null ? new byte[0]
                : threadSpecificContext
                        .getKcopSpecifixContext()
                        .getKeySerializer()
                        .serialize(topic, kafkaAvroKeyGenericRecord);

        // For the Insert generate the bytes to place in the ProducerRecord for the
        // Value field. Note that the serializer employed here is registering the Avro
        // Generic Data Record with the Confluent schema registry transparently.
        GenericRecord kafkaAvroValueGenericRecord = genericRecordRebuilder
                .rebuild(kafkaKcopOperationIn.getKafkaAvroValueGenericRecord());
        byte[] kafkaAvroValueByteArray = threadSpecificContext
                .getKcopSpecifixContext()
                .getValueSerializer()
                .serialize(topic, kafkaAvroValueGenericRecord);

        // Create a NEW ProducerRecord object which will ultimately be written to the
        // Kafka topic and partition specified in the ProducerRecord.
        Integer partition = threadSpecificContext
                .partition(
                        topic,
                        kafkaKcopOperationIn.getPartition(),
                        kafkaAvroKeyGenericRecord);
        List<Header> headers = threadSpecificContext
                .createHeaderList(
                        kafkaKcopReplicationCoordinator,
                        kafkaKcopOperationIn,
                        kafkaAvroKeyGenericRecord,
                        kafkaAvroValueGenericRecord);
        insertKafkaAvroProducerRecord = new ProducerRecord<>(
                topic,
                partition,
                (kafkaAvroKeyByteArray.length != 0) ? kafkaAvroKeyByteArray : null,
                (kafkaAvroValueByteArray.length != 0) ? kafkaAvroValueByteArray : null,
                headers);

        return insertKafkaAvroProducerRecord;
    }

    /**
     * 
     * @param kafkaKcopOperationIn      - Contains information relevant to the
     *                                  current operation being processed.
     * @param threadSpecificContext     - Contains subscription scope information
     *                                  including storage for each thread's
     *                                  non-threadsafe objects, in this case our
     *                                  serializers.
     * @param kafkaAvroKeyGenericRecord - The Avro Record to create a delete Kafka
     *                                  Reocord for.
     * @return producer record
     * @throws UserExitException
     */
    protected ProducerRecord<byte[], byte[]> createDefaultAvroBinaryDeleteProducerRecord(
            KafkaKcopReplicationCoordinatorIF kafkaKcopReplicationCoordinator,
            KafkaKcopOperationInIF kafkaKcopOperationIn,
            ThreadSpecificContext<PersistentSerializerObject> threadSpecificContext,
            GenericRecord kafkaAvroKeyGenericRecord) throws UserExitException {
        ProducerRecord<byte[], byte[]> deleteKafkaProducerRecord = null;
        
        // Wait for a random time between 5 and 10 seconds
        java.util.Random random = new java.util.Random();
        int delay = 5000 + random.nextInt(5000); // 5000ms (5s) to 10000ms (10s)
        try {
            kafkaKcopReplicationCoordinator.logEvent("Waiting for " + (delay / 1000) + " seconds...");
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Handle interruption as appropriate for your context
        }

        // For the Delete generate the bytes to place in the ProducerRecord for the Key
        // field.
        // Note that the key being specified logically represents the "row" being
        // deleted on the source table.
        // The null case is handled although the documented default Kafka behavior will
        // not send one. If there is no Key a delete would delete nothing, so the record
        // to Kafka in that case would be essentially a no-op from the perspective of
        // the consumer.
        // Note that the serializer employed here is registering the Avro Generic Data
        // Record with the Confluent schema registry transparently.
        String topic = threadSpecificContext.remap(kafkaKcopOperationIn.getKafkaTopicName());
        byte[] kafkaAvroKeyByteArray = kafkaAvroKeyGenericRecord == null ? new byte[0]
                : threadSpecificContext
                        .getKcopSpecifixContext()
                        .getKeySerializer()
                        .serialize(topic, kafkaAvroKeyGenericRecord);

        // A delete in Kafka is indicated by specifying that the Value bytes are null.
        // Upon compaction, Kafka then interprets the key as indicating
        // that any former values associated with this key are to be deleted. The user
        // topic and partition specified should correlate to the pairing where
        // the record being deleted was written.
        Integer partition = threadSpecificContext
                .partition(
                        topic,
                        kafkaKcopOperationIn.getPartition(),
                        kafkaAvroKeyGenericRecord);
        GenericRecord kafkaAvroValueGenericRecord = handleBeforeValueOrValueGenericRecord(kafkaKcopOperationIn);

        List<Header> headers = threadSpecificContext
                .createHeaderList(
                        kafkaKcopReplicationCoordinator,
                        kafkaKcopOperationIn,
                        kafkaAvroKeyGenericRecord,
                        kafkaAvroValueGenericRecord);

        deleteKafkaProducerRecord = createDeleteProducerRecord(
                deleteKafkaProducerRecord,
                topic,
                kafkaAvroKeyByteArray,
                partition, headers);

        return deleteKafkaProducerRecord;
    }

    protected ProducerRecord<byte[], byte[]> createDeleteProducerRecord(
            ProducerRecord<byte[], byte[]> deleteKafkaProducerRecord,
            String topic,
            byte[] kafkaAvroKeyByteArray,
            Integer partition,
            List<Header> headers) throws UserExitException {

        deleteKafkaProducerRecord = new ProducerRecord<>(
                topic,
                partition,
                (kafkaAvroKeyByteArray.length != 0) ? kafkaAvroKeyByteArray : null,
                null,
                headers);
        return deleteKafkaProducerRecord;
    }

    protected GenericRecord handleBeforeValueOrValueGenericRecord(KafkaKcopOperationInIF kafkaKcopOperationIn)
            throws UserExitException {
        GenericRecord kafkaAvroGenericRecord = genericRecordRebuilder
                .rebuild(kafkaKcopOperationIn.getKafkaAvroValueGenericRecord());
        return kafkaAvroGenericRecord;
    }

    protected void handleNullKeyRecord(KafkaKcopOperationInIF kafkaKcopOperationIn) throws UserExitException {
        // throw the exception when a table does not contain a key column
        throw new UserExitException("The source table associated with the topic "
                + kafkaKcopOperationIn.getKafkaTopicName() + " does not contain a key column");
    }

    @Override
    public void finish(KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) {
        // No need for any particular finish logic in this example. An optional event
        // log message could be generated if desired.
        kafkaKcopCoordinator.logEvent("Now gracefully exiting KCOP: " + kafkaKcopCoordinator.getUserExitClassName());
    }
}
