package match;

import hermes.dataobj.Event;
import hermes.dataobj.Subscription;
import hermes.matching.Matcher;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Set;


/**
 * Created by hult on 3/24/17.
 */
public class MatcherBolt extends BaseBasicBolt{
    Matcher matcher;
    int eCounter;
    int taskID = 0;
    int attrSize;
    int nMatcherPerBolt;

    public MatcherBolt(int attrSize, int nMatcherPerBolt) {
        this.attrSize = attrSize;
        this.nMatcherPerBolt = nMatcherPerBolt;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.matcher = new Matcher(this.attrSize);
        eCounter = 0;
        this.taskID = context.getThisTaskId();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (input.contains(Constant.EVENT_FIELD)) {
            Event e = (Event) input.getValueByField(Constant.EVENT_FIELD);
            Set<Integer> matched = matcher.match(e);
            //System.out.printf("%s on %9d, processed %9d, matched: %5d\n", toString(), e.id, eCounter++, matched.size());
            collector.emit(new Values("event", e.id, matched.size(), e.timestamp));
        } else if (input.contains(Constant.SUB_FIELD)) {
            Subscription sub = (Subscription)input.getValueByField(Constant.SUB_FIELD);
            matcher.addSubscription(sub);
            collector.emit(new Values("sub", sub.id, 0, sub.timestamp));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("type", "id", "size", "latency"));
    }

    @Override
    public String toString() {
        return String.format("MATCHER_%02d", taskID);
    }
}
