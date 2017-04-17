package match;

import hermes.matching.Dispatcher;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseBasicBolt;

import java.util.*;

/**
 * Created by hult on 4/2/17.
 */
abstract class AbstractDispatcherBolt extends BaseBasicBolt{
    protected int attrSize;
    protected int nSeg;

    protected Dispatcher dispatcher;
    protected int N_MATCHER;
    protected List<Integer> taskIDs;
    protected int N_SPACE;
    protected int N_SPACE_PER_BOLT;
    protected int N_SPACE_PER_MATCHER;
    protected int N_MATCHER_PER_BOLT;
    protected Set<Integer> processed;
    protected Map<Integer, Set<Integer>> id2vec;

    public AbstractDispatcherBolt(int attrSize, int nSeg, int nMatcher) {
        this.attrSize = attrSize;
        this.nSeg = nSeg;
        this.N_MATCHER_PER_BOLT = nMatcher;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        System.out.println("instance: " + toString());
        this.dispatcher = new Dispatcher(attrSize,nSeg, false);
        this.taskIDs = new ArrayList(context.getComponentTasks(Constant.MATCHER));
        this.N_MATCHER = this.taskIDs.size();
        this.processed = new HashSet<>();
        this.N_SPACE = (int) Math.pow(nSeg, attrSize);
        this.N_SPACE_PER_BOLT = this.N_SPACE / this.N_MATCHER;
        this.N_SPACE_PER_MATCHER = this.N_SPACE_PER_BOLT / this.N_MATCHER_PER_BOLT;
        this.id2vec = new HashMap<>();
        System.out.println("-------------" + this.taskIDs + " matcher---------");
    }
}
