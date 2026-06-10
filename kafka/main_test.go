package kafka_test

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/nedcg/juzu"
	"github.com/nedcg/juzu/kafka"
	"github.com/nedcg/juzu/kafka/saramax"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
)

var testBrokers []string

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Spin up Redpanda container (fast, Kafka-compatible C++ broker)
	container, err := redpanda.Run(
		ctx,
		"docker.redpanda.com/redpandadata/redpanda:v23.3.3",
	)
	if err != nil {
		log.Fatalf("failed to start redpanda: %s", err)
	}

	// Get seed broker address for Sarama connection
	brokerAddr, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		log.Fatalf("failed to get kafka seed broker: %s", err)
	}
	testBrokers = []string{brokerAddr}

	// Run tests
	code := m.Run()

	// Terminate container
	if err := testcontainers.TerminateContainer(container); err != nil {
		log.Fatalf("failed to terminate container: %s", err)
	}

	os.Exit(code)
}

func TestKafkaE2E(t *testing.T) {
	if len(testBrokers) == 0 {
		t.Skip("Redpanda broker not available")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	// 1. Topic definitions
	inputTopic := "e2e.input"
	outputTopic := "e2e.output"
	dlqTopic := inputTopic + "-dlq"
	stateTopic := "e2e.state"

	createTopics(t, testBrokers, []string{inputTopic, outputTopic, dlqTopic}, stateTopic)

	// 2. Client Configurations
	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	config.Producer.Transaction.ID = "e2e-tx-producer"
	config.Producer.Return.Successes = true

	// Build transactional producer
	producer, err := sarama.NewSyncProducer(testBrokers, config)
	require.NoError(t, err)
	defer producer.Close()

	// Build state store (CompactedTopicStore)
	stateStoreProducerConfig := sarama.NewConfig()
	stateStoreProducerConfig.Version = sarama.V3_0_0_0
	stateStoreProducerConfig.Producer.Return.Successes = true
	stateStoreProducer, err := sarama.NewSyncProducer(testBrokers, stateStoreProducerConfig)
	require.NoError(t, err)
	defer stateStoreProducer.Close()

	stateStore := saramax.NewCompactedTopicStore(stateStoreProducer, stateTopic)

	// Start compacted topic reader in background
	consumerClientConfig := sarama.NewConfig()
	consumerClientConfig.Version = sarama.V3_0_0_0
	consumerClient, err := sarama.NewConsumer(testBrokers, consumerClientConfig)
	require.NoError(t, err)
	defer consumerClient.Close()

	err = stateStore.StartReader(ctx, consumerClient)
	require.NoError(t, err)

	// GroupID extractor helper
	getGroupID := func(msg kafka.Message) string {
		return string(msg.Key)
	}

	// 3. Define the mutable business handler
	var handlerErr error = errors.New("db insert constraint failed")
	var handlerErrMu sync.RWMutex

	setHandlerErr := func(err error) {
		handlerErrMu.Lock()
		defer handlerErrMu.Unlock()
		handlerErr = err
	}

	getHandlerErr := func() error {
		handlerErrMu.RLock()
		defer handlerErrMu.RUnlock()
		return handlerErr
	}

	businessHandler := func(ctx *kafka.BatchContext, msg kafka.Message) error {
		val := string(msg.Value)
		if val == "fail" {
			if err := getHandlerErr(); err != nil {
				return err
			}
		}
		// Write business outputs inside the transaction
		ctx.Send(outputTopic, msg.Key, []byte("processed-"+val))
		return nil
	}

	// 4. Produce test messages to inputTopic
	// M1: GroupA -> "ok-1" (succeeds)
	// M2: GroupA -> "fail" (fails, poisons GroupA)
	// M3: GroupA -> "ok-2" (should be skipped/cascade-poisoned)
	// M4: GroupB -> "ok-3" (succeeds)
	produce(t, testBrokers, inputTopic, []sarama.ProducerMessage{
		{Key: sarama.StringEncoder("GroupA"), Value: sarama.StringEncoder("ok-1")},
		{Key: sarama.StringEncoder("GroupA"), Value: sarama.StringEncoder("fail")},
		{Key: sarama.StringEncoder("GroupA"), Value: sarama.StringEncoder("ok-2")},
		{Key: sarama.StringEncoder("GroupB"), Value: sarama.StringEncoder("ok-3")},
	})

	// 5. Start main consumer group
	consumerConfig := sarama.NewConfig()
	consumerConfig.Version = sarama.V3_0_0_0
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}

	pipeline := []juzu.Interceptor[*kafka.BatchContext]{
		kafka.PoisonFilter(stateStore, getGroupID),
		kafka.WrapHandler(stateStore, getGroupID, businessHandler),
	}

	saramaProducerAdapter := saramax.NewSaramaProducer(producer)
	genericConsumer := kafka.NewBatchConsumer(kafka.BatchConsumerConfig{
		GroupID:         "e2e-main-group",
		Producer:        saramaProducerAdapter,
		Pipeline:        pipeline,
		StateTopic:      stateTopic,
		UseTransactions: true,
	})

	consumerHandler := saramax.NewConsumerGroupHandler(
		genericConsumer,
		100,
		50*time.Millisecond,
	)

	consumerGroup, err := sarama.NewConsumerGroup(testBrokers, "e2e-main-group", consumerConfig)
	require.NoError(t, err)
	defer consumerGroup.Close()

	// Run consumer group in background
	cgCtx, cgCancel := context.WithCancel(ctx)
	defer cgCancel()
	go func() {
		for {
			if err := consumerGroup.Consume(cgCtx, []string{inputTopic}, consumerHandler); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Printf("Consumer group error: %v", err)
			}
			if cgCtx.Err() != nil {
				return
			}
		}
	}()

	// Wait for processing to complete (check DLQ count)
	waitForDLQCount(t, testBrokers, dlqTopic, 2, 15*time.Second)

	// Verify GroupA is poisoned, GroupB is healthy
	assert.True(t, stateStore.IsPoisoned("GroupA"))
	assert.False(t, stateStore.IsPoisoned("GroupB"))

	// Verify business outputs sent to outputTopic so far:
	// "processed-ok-1" (GroupA) and "processed-ok-3" (GroupB)
	outputs := readAllMessages(t, testBrokers, outputTopic, 10*time.Second)
	assert.Len(t, outputs, 2)
	assert.Contains(t, outputs, "processed-ok-1")
	assert.Contains(t, outputs, "processed-ok-3")

	// 6. Produce new message M5: GroupA -> "ok-4"
	// It should immediately bypass processing and go to DLQ because GroupA is poisoned.
	produce(t, testBrokers, inputTopic, []sarama.ProducerMessage{
		{Key: sarama.StringEncoder("GroupA"), Value: sarama.StringEncoder("ok-4")},
	})

	// Wait for DLQ count to reach 3
	waitForDLQCount(t, testBrokers, dlqTopic, 3, 15*time.Second)

	dlqMsgs := readAllMessages(t, testBrokers, dlqTopic, 10*time.Second)
	assert.Len(t, dlqMsgs, 3) // fail, ok-2, ok-4

	// 7. Reprocessing Phase (Fix the bug + run reprocessor)
	setHandlerErr(nil) // Fix the bug (handlerErr = nil)

	// Stop main consumer group to avoid rebalance conflicts on state topic
	cgCancel()

	// Clear/Unpoison GroupA in the state store
	err = stateStore.Unpoison("GroupA")
	require.NoError(t, err)

	// Verify GroupA is unpoisoned
	assert.False(t, stateStore.IsPoisoned("GroupA"))

	// Create transactional producer for reprocessor
	reprocProducerConfig := sarama.NewConfig()
	reprocProducerConfig.Version = sarama.V3_0_0_0
	reprocProducerConfig.Producer.RequiredAcks = sarama.WaitForAll
	reprocProducerConfig.Producer.Idempotent = true
	reprocProducerConfig.Net.MaxOpenRequests = 1
	reprocProducerConfig.Producer.Transaction.ID = "e2e-reproc-producer"
	reprocProducerConfig.Producer.Return.Successes = true

	reprocProducer, err := sarama.NewSyncProducer(testBrokers, reprocProducerConfig)
	require.NoError(t, err)
	defer reprocProducer.Close()

	// Initialize reprocessor
	reprocPipeline := []juzu.Interceptor[*kafka.BatchContext]{
		kafka.WrapHandler(stateStore, getGroupID, businessHandler),
	}

	reprocProducerAdapter := saramax.NewSaramaProducer(reprocProducer)
	genericReproc := kafka.NewReprocessor(
		"e2e-reproc-group",
		dlqTopic,
		stateStore,
		getGroupID,
		reprocProducerAdapter,
		reprocPipeline,
	)

	reprocessor := saramax.NewReprocessor(
		genericReproc,
		10,
		50*time.Millisecond,
	)

	reprocGroup, err := sarama.NewConsumerGroup(testBrokers, "e2e-reproc-group", consumerConfig)
	require.NoError(t, err)
	defer reprocGroup.Close()

	reprocCtx, reprocCancel := context.WithCancel(ctx)
	defer reprocCancel()
	go func() {
		for {
			if err := reprocGroup.Consume(reprocCtx, []string{dlqTopic}, reprocessor); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Printf("Reprocessor group error: %v", err)
			}
			if reprocCtx.Err() != nil {
				return
			}
		}
	}()

	// Wait for reprocessing outputs to land in outputTopic
	// outputTopic should now have: "processed-ok-1", "processed-ok-3" + "processed-fail", "processed-ok-2", "processed-ok-4"
	waitForOutputCount(t, testBrokers, outputTopic, 5, 20*time.Second)

	finalOutputs := readAllMessages(t, testBrokers, outputTopic, 10*time.Second)
	assert.Len(t, finalOutputs, 5)
	assert.Contains(t, finalOutputs, "processed-fail")
	assert.Contains(t, finalOutputs, "processed-ok-2")
	assert.Contains(t, finalOutputs, "processed-ok-4")
}

// Helpers

func createTopics(t *testing.T, brokers []string, topics []string, compactedTopic string) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0
	admin, err := sarama.NewClusterAdmin(brokers, config)
	require.NoError(t, err)
	defer admin.Close()

	for _, topic := range topics {
		err = admin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     3,
			ReplicationFactor: 1,
		}, false)
		require.NoError(t, err)
	}

	err = admin.CreateTopic(compactedTopic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
		ConfigEntries: map[string]*string{
			"cleanup.policy": stringPtr("compact"),
		},
	}, false)
	require.NoError(t, err)
}

func stringPtr(s string) *string {
	return &s
}

func produce(t *testing.T, brokers []string, topic string, msgs []sarama.ProducerMessage) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(brokers, config)
	require.NoError(t, err)
	defer producer.Close()

	for _, m := range msgs {
		m.Topic = topic
		_, _, err = producer.SendMessage(&m)
		require.NoError(t, err)
	}
}

func readAllMessages(t *testing.T, brokers []string, topic string, timeout time.Duration) []string {
	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumer(brokers, config)
	require.NoError(t, err)
	defer consumer.Close()

	partitions, err := consumer.Partitions(topic)
	require.NoError(t, err)

	var mu sync.Mutex
	var values []string
	var wg sync.WaitGroup

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	for _, partition := range partitions {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		require.NoError(t, err)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer pc.Close()
			defer wg.Done()
			for {
				select {
				case msg := <-pc.Messages():
					mu.Lock()
					values = append(values, string(msg.Value))
					mu.Unlock()
				case <-ctx.Done():
					return
				}
			}
		}(pc)
	}

	wg.Wait()
	return values
}

func waitForDLQCount(t *testing.T, brokers []string, topic string, expected int, timeout time.Duration) {
	start := time.Now()
	for time.Since(start) < timeout {
		msgs := readAllMessages(t, brokers, topic, 1*time.Second)
		if len(msgs) >= expected {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for DLQ count to reach %d", expected)
}

func waitForOutputCount(t *testing.T, brokers []string, topic string, expected int, timeout time.Duration) {
	start := time.Now()
	for time.Since(start) < timeout {
		msgs := readAllMessages(t, brokers, topic, 1*time.Second)
		if len(msgs) >= expected {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for Output count to reach %d", expected)
}
