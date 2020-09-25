//
// Created by admin on 2019-06-24.
//

#include "KafkaProducer.h"
#include "log.h"

void ProducerEventCb::event_cb(RdKafka::Event &event) {
    switch (event.type()) {
        case RdKafka::Event::EVENT_ERROR:
            LOG_ERROR << RdKafka::err2str(event.err()) << ": " << event.str();
            break;

        case RdKafka::Event::EVENT_STATS:
            LOG_INFO << "\"STATS\": " << event.str();
            break;

        default:
            LOG_INFO << "EVENT " << event.type() <<
                     " (" << RdKafka::err2str(event.err()) << "): " << event.str();
            break;
    }
}

void ProducerDeliveryReportCb::dr_cb(RdKafka::Message &message) {
    if (message.err()) {
    }
}

KafkaClientProducer::KafkaClientProducer(const std::string &brokers, const std::string &topicStr) :
        m_brokers(brokers), m_topicStr(topicStr), partition(RdKafka::Topic::PARTITION_UA) {

    Init();
}

bool KafkaClientProducer::Init() {
    conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    std::string errstr;
    conf->set("metadata.broker.list", m_brokers, errstr);
    conf->set("event_cb", &eventCb, errstr);
    conf->set("dr_cb", &deliveryReportCb, errstr);

    producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        LOG_ERROR << "Failed to create producer: " << errstr;
        return false;
    }

    LOG_SPCL << "% Created producer " << producer->name();

    topic = RdKafka::Topic::create(producer, m_topicStr,
                                   tconf, errstr);
    if (!topic) {
        LOG_ERROR << "Failed to create topic: " << errstr;
        return false;
    }
    LOG_SPCL << "% Created topic " << topic->name();

    std::string blockMS;
    conf->get("queue.buffering.max.ms", blockMS);
    LOG_SPCL << "blockMS in global conf: " << blockMS;
    tconf->get("queue.buffering.max.ms", blockMS);
    LOG_SPCL << "blockMS in topic conf: " << blockMS;

    return true;
}

void KafkaClientProducer::PushDataToKafka(const std::vector<std::string> &contents) {
    try {
        if (contents.empty()) {
            LOG_ERROR << "contests is empty";
            return;
        }
        long count = 0;
        std::string docid = "";
        for (auto &content : contents) {
            docid.append(content);
            ++count;
            if (count >= 100) {
                count = 0;
                RdKafka::ErrorCode resp =
                        producer->produce(topic, partition,
                                          RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                                          const_cast<char *>(docid.c_str()), docid.size(),
                                          NULL, NULL);//只是将内容加入到本地队列中
                if (resp != RdKafka::ERR_NO_ERROR) {
                    LOG_ERROR << "produce message error: " << resp << " ; error docid " << docid;
                }
                producer->poll(0);
                docid.clear();
                count = 0;
                continue;
            }
            docid.append(",");
        }
        if (!docid.empty()) {
            docid.pop_back();
            RdKafka::ErrorCode resp =
                    producer->produce(topic, partition,
                                      RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                                      const_cast<char *>(docid.c_str()), docid.size(),
                                      NULL, NULL);//只是将内容加入到本地队列中
            if (resp != RdKafka::ERR_NO_ERROR) {
                LOG_ERROR << "produce message error: " << resp << " ; error docid " << docid;
            }
            producer->poll(0);
        }

//        while (producer->outq_len() > 0) {
////            LOG_INFO << "Waiting for " << producer->outq_len();
//            producer->poll(1000);
//        }

        //int64_t end = ml_platform::TimeUtil::CurrentTimeInMilliseconds();

        //std::cout << "time = " << (end - begin) << std::endl;


    } catch (std::exception &exception) {
        LOG_ERROR << "produce message exception: " << exception.what();
    }
}

void KafkaClientProducer::PushDataToKafka(const std::string &content) {
    try {
        if (content.empty()) {
            LOG_ERROR << "contest is empty";
            return;
        }
        long count = 0;
        if (!content.empty()) {
            RdKafka::ErrorCode resp =
                    producer->produce(topic, partition,
                                      RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                                      const_cast<char *>(content.c_str()), content.size(),
                                      NULL, NULL);//只是将内容加入到本地队列中

            if (resp != RdKafka::ERR_NO_ERROR) {
                LOG_ERROR << "produce message error: " << resp << " ; error docid " << content;
            }
            producer->poll(0);
        }
    } catch (std::exception &exception) {
        LOG_ERROR << "produce message exception: " << exception.what();
    }
}

KafkaClientProducer::~KafkaClientProducer() {
    delete topic;
    delete producer;
}
