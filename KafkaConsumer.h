//
// Created by admin on 2019-06-20.
//

#ifndef DOC_FEATURE_SERVER_KAFKA_KAFKACONSUMER_H
#define DOC_FEATURE_SERVER_KAFKA_KAFKACONSUMER_H

#include <memory>
#include <iostream>
#include <rdkafkacpp.h>
#include <tbb/concurrent_unordered_map.h>

#include "log.h"

struct KafkaConfig_ {
    std::string brokers;
    std::string topic;
    std::string groupId;
    int64_t startOffset; //end
    int offline;
    int consumeTimeout;
};

class ExampleEventCb : public RdKafka::EventCb {
 public:
  void event_cb(RdKafka::Event &event);
};

class ExampleRebalanceCb : public RdKafka::RebalanceCb {
 private:
  //static void part_list_print (const std::vector<RdKafka::TopicPartition*>&partitions);
  tbb::concurrent_unordered_map<int, int64_t>& m_partitionToOffset;

 public:
  ExampleRebalanceCb(tbb::concurrent_unordered_map<int, int64_t>& partitionToOffset) : m_partitionToOffset(partitionToOffset) {}
  void rebalance_cb (RdKafka::KafkaConsumer *consumer,
                     RdKafka::ErrorCode err,
                     std::vector<RdKafka::TopicPartition*> &partitions);
};

class KafkaConsumer {
 public:
  KafkaConsumer(const KafkaConfig_& kafkaConfig);
  void consumeMsg();
  void destructMsg(RdKafka::Message* msg);
  bool isRunning() { return m_isRunning; }
  bool isFinished() { return m_isFinished; }
  void restart();
  void stop();
  int64_t getConsumerLag();
  int64_t getMessageCount() { return m_messageCount; }

  int64_t m_messageCount;
  bool m_isRunning = true;

 private:
  void init();

  KafkaConfig_ m_kafkaConfig;
  RdKafka::KafkaConsumer* m_consumer = NULL;
  ExampleRebalanceCb exampleRebalanceCb; // Note: MUST no destructed during consuming, or else segment error!!!
  ExampleEventCb exampleEventCb;         // Note: MUST no destructed during consuming, or else segment error!!!

  int64_t m_messageTotalSize;
  int m_consumeRound;
  tbb::concurrent_unordered_map<int, int64_t> m_partitionToOffset;

  volatile bool m_isFinished = false;
};


#endif //DOC_FEATURE_SERVER_KAFKA_KAFKACONSUMER_H
