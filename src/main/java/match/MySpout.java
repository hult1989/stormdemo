package match;

import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.RawScheme;
import org.apache.storm.spout.SchemeAsMultiScheme;

import java.util.Properties;

/**
 * Created by hult on 4/2/17.
 */
public class MySpout {
    public static KafkaSpout createEventSpout(String zooHost) {
        ZkHosts zkHosts = new ZkHosts(zooHost);
        SpoutConfig config = new SpoutConfig(zkHosts, Constant.EVENT_TOPIC_NAME, "/event", "event-spout");
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        config.scheme = new SchemeAsMultiScheme(new RawScheme());
        return new KafkaSpout(config);
    }

    public static KafkaSpout createSubSpout(String zooHost) {
        ZkHosts zkHosts = new ZkHosts(zooHost);
        SpoutConfig config = new SpoutConfig(zkHosts, Constant.SUB_TOPIC_NAME, "/subscription", "sub-spout");
        config.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        config.scheme = new SchemeAsMultiScheme(new RawScheme());
        return new KafkaSpout(config);
    }

    public static KafkaBolt getKafkaProducerBolt(String host) {
    Properties props = new Properties();
    props.put("bootstrap.servers", host+":9092");
    props.put("acks", "1");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    return new KafkaBolt()
            .withProducerProperties(props)
            .withTopicSelector(new DefaultTopicSelector(Constant.RESULT_TOPIC_NAME))
            .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());

    }
}
