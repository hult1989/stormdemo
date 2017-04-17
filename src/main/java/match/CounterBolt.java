package match;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Created by hult on 3/25/17.
 */
public class CounterBolt extends BaseBasicBolt{

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String type = input.getStringByField("type");
        int id = input.getIntegerByField("id");
        int size = input.getIntegerByField("size");
        long now = System.currentTimeMillis();
        long latency = now - input.getLongByField("latency");
        System.out.printf("%s\t%s\t%d\t%d\t%d\n",now, type, id, size, latency);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
