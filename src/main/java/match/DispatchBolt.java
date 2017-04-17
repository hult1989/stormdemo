package match;

import hermes.dataobj.Event;
import hermes.matching.Dispatcher;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.*;

/**
 * Created by hult on 3/23/17.
 */
public class DispatchBolt extends BaseBasicBolt{
    Dispatcher dispatcher;
    List<Integer> taskIDs;
    Set<Integer> processed;
    static final int SIZE  =10;
    static final int nSeg = 4;
    static final int TOTAL = (int) Math.pow(nSeg, SIZE);
    static int nMatcher;
    int dup;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        System.out.println("instance: " + toString());
        this.dispatcher = new Dispatcher(SIZE,nSeg, false);
        this.taskIDs = new ArrayList(context.getComponentTasks("matcher"));
        this.nMatcher = this.taskIDs.size();
        this.processed = new HashSet<>();
        this.dup += 1;
        System.out.println("-------------" + this.taskIDs + " matcher---------");
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (input.getSourceComponent().equals("EVENT_SPOUT")) {
            byte[] bytes = input.getBinary(0);
            Event event = Event.fromBytes(bytes);
            //System.out.println(event.toString());
            event.timestamp = System.currentTimeMillis();
            List<Integer> vectors = dispatcher.vectorOfEvent(event.values);

            //collector.emit(new Values(event.id, result.size()));
            //System.out.println("EVENT: " + event.id);
        } else if (input.getSourceComponent().equals("SUBSCRIPTION_SPOUT")) {

        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("eventStream", true, new Fields("event"));
        declarer.declareStream("subStream", true, new Fields("subscription"));
    }
}
