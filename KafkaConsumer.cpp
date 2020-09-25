//
// Created by admin on 2019-06-20.
//

#include "KafkaConsumer.h"

void ExampleEventCb::event_cb(RdKafka::Event &event) {

}

// rebalance时这个方法会被回调
// 自动触发，刚启动时会有，其它时机未知
void ExampleRebalanceCb::rebalance_cb(RdKafka::KafkaConsumer *consumer, RdKafka::ErrorCode err,
                                      std::vector<RdKafka::TopicPartition *> &partitions) {
  if (err == RdKafka::ERR__ASSIGN_PARTITIONS)
  {
    m_partitionToOffset.clear();

    //static int64_t rebalanceOffset = m_kafkaConfig.startOffset;
    static int64_t rebalanceOffset = 0;
    int64_t nextOffset = rebalanceOffset;

    LOG_INFO << "ERR__ASSIGN_PARTITIONS" << std::endl;

    // 设置rebalance的offset，初始值是配置里的startOffset，后面就变成STORED
    for (size_t i = 0; i < partitions.size(); ++i)
    {
      LOG_INFO << "ERR__ASSIGN_PARTITIONS " << partitions[i]->partition() << " at offset " << nextOffset << std::endl;
      partitions[i]->set_offset(nextOffset);
      m_partitionToOffset[partitions[i]->partition()] = nextOffset;
    }
    consumer->assign(partitions);

    //if(rebalanceOffset == m_kafkaConfig.startOffset){
    if(rebalanceOffset == 0){
      rebalanceOffset = RdKafka::Topic::OFFSET_STORED;
    }

  }
  else
  {
    if (err != RdKafka::ERR__REVOKE_PARTITIONS)
    {
      LOG_INFO << "rebalance_cb error" << std::endl;
    }
    LOG_INFO << "consumer->unassign()" << std::endl;
    consumer->unassign();
    //m_partitionCount = 0;
  }
}

KafkaConsumer::KafkaConsumer(const KafkaConfig_ &kafkaConfig) : exampleRebalanceCb(m_partitionToOffset) {
  m_kafkaConfig = kafkaConfig;
  m_consumeRound = kafkaConfig.offline;
  m_messageCount = 0;
  init();
}

void KafkaConsumer::init() {
  LOG_INFO << "Broker=" << m_kafkaConfig.brokers.c_str() << std::endl;
  std::string errstr;

  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

  conf->set("rebalance_cb", &exampleRebalanceCb, errstr);
  if (conf->set("group.id", m_kafkaConfig.groupId, errstr) != RdKafka::Conf::CONF_OK) {
    LOG_ERROR << errstr << std::endl;
    exit(1);
  }

  conf->set("metadata.broker.list", m_kafkaConfig.brokers, errstr);
  conf->set("event_cb", &exampleEventCb, errstr);
  conf->set("log.connection.close", "false", errstr);
  conf->set("statistics.interval.ms", "30000", errstr);

  // 离线训练需要手动去向server汇报当前消费的offset
  // 这是出于后面手动调整offset的考虑，因为如果设置自动汇报offset后面再手动修改offset实测无法生效
  if (m_kafkaConfig.startOffset != RdKafka::Topic::OFFSET_END) {
    conf->set("auto.commit.enable", "false", errstr);
    conf->set("enable.auto.offset.store", "false", errstr);
  }

  m_consumer = RdKafka::KafkaConsumer::create(conf, errstr);
  if (!m_consumer) {
    LOG_ERROR << "Failed to create consumer: " << errstr << std::endl;
    exit(1);
  }
  delete conf;
  LOG_INFO << "Created consumer " << m_consumer->name() << std::endl;

  std::vector<std::string> topics;
  topics.push_back(m_kafkaConfig.topic);
  RdKafka::ErrorCode err = m_consumer->subscribe(topics);
  if (err) {
    LOG_ERROR << "Failed to subscribe to " << topics.size() << " topics: "
               << RdKafka::err2str(err) << std::endl;
    exit(1);
  }
}

void KafkaConsumer::consumeMsg() {
  RdKafka::Message* message = m_consumer->consume(m_kafkaConfig.consumeTimeout);

  switch (message->err())
  {
    case RdKafka::ERR__TIMED_OUT:
      LOG_ERROR << "Consume kafka message timeout." << std::endl;
      break;

    case RdKafka::ERR_NO_ERROR:
      ++m_messageCount;
      /* Real message */
      std::cout << "ERR_NO_ERROR" << std::endl;
      std::cout << "Read msg at offset " << message->offset() << " : " << static_cast<const char *>(message->payload()) << std::endl;
          std::cout << "Read content: " << std::string((const char*)message->payload()) << std::endl;
          std::cout << "Read content tm: " << message->timestamp().timestamp << std::endl;
      break;

    case RdKafka::ERR__PARTITION_EOF:
      /* Last message */
      std::cout << "EOF reached for all : mes size = " << m_messageCount << std::endl;
      m_isRunning = false;
      break;

    case RdKafka::ERR__UNKNOWN_TOPIC:
    case RdKafka::ERR__UNKNOWN_PARTITION:
      std::cout << "Consume failed: " << message->errstr() << std::endl;
      m_isRunning = false;
      break;

    default:
      /* Errors */
      std::cout << "Consume failed: " << message->errstr() << std::endl;
      m_isRunning = false;

  }

}

int64_t KafkaConsumer::getConsumerLag() {
  // compute all partitions' lag
  int64_t low = 0, high = 0;
  int64_t totalConsumerLag = 0, consumerLagCount = 0;
  for (auto iter = m_partitionToOffset.begin(); iter != m_partitionToOffset.end(); iter++) {
    m_consumer->query_watermark_offsets(m_kafkaConfig.topic, iter->first, &low, &high, 50);
    if (high > 0) {
      totalConsumerLag += (high - iter->second);
      consumerLagCount++;
    }
  }

  if (consumerLagCount != 0 ) {
    return totalConsumerLag / consumerLagCount;
  } else {
    return -1;
  }
}

void KafkaConsumer::destructMsg(RdKafka::Message* message) {
  if (message) {
    delete message;
  }
}

void KafkaConsumer::stop() {
  m_consumer->close();
  delete m_consumer;
  LOG_INFO << "Consumed " << m_messageCount << " messages (" << m_messageTotalSize << " bytes)" << std::endl;
  RdKafka::wait_destroyed(5000);
}

void KafkaConsumer::restart() {
  stop();
  init();
}
