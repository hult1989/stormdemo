import match.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by hult on 3/7/17.
 */
public class Topology {

    public static void main(String[] args) throws IOException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        Properties properties = new Properties();
        properties.load(new FileInputStream("storm.config"));
        String zooHost = properties.getProperty("zooHost");
        int nEventKafka = Integer.parseInt(properties.getProperty("nEventKafka"));
        int nSubKafka = Integer.valueOf(properties.getProperty("nSubKafka"));
        int nEventDispatcher = Integer.valueOf(properties.getProperty("nEventDispatcher"));
        int nSubDispatcher = Integer.valueOf(properties.getProperty("nSubDispatcher"));
        int nMatcherBolt = Integer.valueOf(properties.getProperty("nMatcherBolt"));
        int nMatcherPerBolt = Integer.valueOf(properties.getProperty("nMatcherPerBolt"));
        int nSeg = Integer.valueOf(properties.getProperty("nSeg"));
        int attrSize = Integer.valueOf(properties.getProperty("attrSize"));
        System.out.println(properties);

        Config conf = new Config();
        conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 10 * 1024);
        conf.setDebug(false);


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(Constant.EVENT_SPOUT, MySpout.createEventSpout(zooHost), nEventKafka);
        builder.setSpout(Constant.SUB_SPOUT, MySpout.createSubSpout(zooHost), nSubKafka);

        builder.setBolt(Constant.EVENT_DISPATCHER, new EventDispatcherBolt(attrSize, nSeg, nMatcherPerBolt), nEventDispatcher)
                .shuffleGrouping(Constant.EVENT_SPOUT);
        builder.setBolt(Constant.SUB_DISPATCHER, new SubDispatcherBolt(attrSize, nSeg, nMatcherPerBolt), nSubDispatcher)
                .shuffleGrouping(Constant.SUB_SPOUT);

        builder.setBolt(Constant.MATCHER, new MatcherBolt(attrSize, nMatcherPerBolt), nMatcherBolt)
                .directGrouping(Constant.EVENT_DISPATCHER, Constant.EVENT_STREAM)
                .directGrouping(Constant.SUB_DISPATCHER, Constant.SUB_STREAM)
                .setMemoryLoad(1024);

        builder.setBolt(Constant.COUNTER, new CounterBolt()).allGrouping(Constant.MATCHER);
        //builder.setBolt(Constant.RESULT, MySpout.getKafkaProducerBolt(zooHost)).allGrouping(Constant.COUNTER);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(Constant.TOPOLOGY_NAME, conf, builder.createTopology());
        //StormSubmitter.submitTopology("hermes_matching", conf, builder.createTopology());
        //cluster.shutdown();
    }
}
