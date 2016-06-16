package inf.ed.gfd.detect.repl;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.PriorityQueue;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class bPar {

	int NUM_MACHINES;
	int NUM_TASKS;

	static Logger log = LogManager.getLogger(bPar.class);

	public Int2ObjectMap<Set<ReplicatedGWorkUnit>> makespan(int NUM_MACHINES,
			PriorityQueue<ReplicatedGWorkUnit> workload) {

		Int2ObjectMap<Set<ReplicatedGWorkUnit>> assignment = new Int2ObjectOpenHashMap<Set<ReplicatedGWorkUnit>>();
		this.NUM_MACHINES = NUM_MACHINES;
		this.NUM_TASKS = workload.size();

		int[] completeTime = new int[NUM_MACHINES];
		int completeAllTime = 0;

		// init complete time array
		for (int i = 0; i < NUM_MACHINES; i++) {
			completeTime[i] = 0;
		}

		while (!workload.isEmpty()) {
			ReplicatedGWorkUnit wu = workload.poll();
			int complete = Integer.MAX_VALUE;
			int assign = 0;
			for (int i = 0; i < NUM_MACHINES; i++) {
				int wouldComplete = completeTime[i] + wu.getET();
				if (wouldComplete < complete) {
					complete = wouldComplete;
					assign = i;
				}
			}
			completeTime[assign] += wu.getET();
			if (complete > completeAllTime) {
				completeAllTime = complete;
			}

			if (!assignment.containsKey(assign)) {
				assignment.put(assign, new ObjectOpenHashSet<ReplicatedGWorkUnit>());
			}
			assignment.get(assign).add(wu);
		}

		System.out.println("estimated longest time =" + completeAllTime);
		return assignment;
	}
}
