//
// Created by admin on 2019-06-24.
//

#ifndef CPPSERVER_KAFKAPRODUCER_H
#define CPPSERVER_KAFKAPRODUCER_H

#include <librdkafka/rdkafkacpp.h>

class ProducerEventCb : public RdKafka::EventCb {
public:
    void event_cb(RdKafka::Event &event);
};

class ProducerDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
    void dr_cb(RdKafka::Message &message);
};

class KafkaClientProducer {
public:
    KafkaClientProducer(const std::string &brokers, const std::string &topicStr);

    bool Init();

    //bool produce(const std::string& content);
    void PushDataToKafka(const std::vector <std::string> &contents);

    void PushDataToKafka(const std::string &content);

    ~KafkaClientProducer();

private:
    std::string m_brokers;
    std::string m_topicStr;
    std::string m_groupId;
    RdKafka::Conf *conf;
    RdKafka::Conf *tconf;
    int32_t partition;
    RdKafka::Producer *producer;
    RdKafka::Topic *topic;
    ProducerEventCb eventCb;
    ProducerDeliveryReportCb deliveryReportCb;
};


#endif //CPPSERVER_KAFKAPRODUCER_H
