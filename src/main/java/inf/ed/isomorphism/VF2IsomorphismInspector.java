package inf.ed.isomorphism;

import static inf.ed.isomorphism.State.NULL_NODE;
import inf.ed.graph.structure.Edge;
import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.Vertex;
import inf.ed.graph.structure.adaptor.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class VF2IsomorphismInspector<V1 extends Vertex, E1 extends Edge, V2 extends Vertex, E2 extends Edge> {

	static Logger log = LogManager.getLogger(VF2IsomorphismInspector.class);

	private Graph<V1, E1> g1;
	private Graph<V2, E2> g2;

	ConcurrentLinkedQueue<Integer> cands;
	ConcurrentLinkedQueue<Integer> results;

	int du;// designate vertex in Q(g1)

	int threadNumLimit = 0;
	int threadTimeoutLimit = 0;

	public VF2IsomorphismInspector() {
	}

	public void rewriteQuery() {
		return;
	}

	public VF2IsomorphismInspector(Graph<V1, E1> query, int du, Graph<V2, E2> graph,
			Collection<Integer> candidates) {

		this.g1 = query;
		this.g2 = graph;

		this.du = du;

		this.results = new ConcurrentLinkedQueue<Integer>();
		this.cands = new ConcurrentLinkedQueue<Integer>();
		this.cands.addAll(candidates);
	}

	public Set<Integer> findIsomorphic() {

		Set<Integer> isomorphics = new HashSet<Integer>();

		int threadNum;
		if (this.threadNumLimit == 0) {
			threadNum = Runtime.getRuntime().availableProcessors();
		} else {
			threadNum = Math.min(this.threadNumLimit, Runtime.getRuntime().availableProcessors());
		}

		List<Thread> threadsPool = new ArrayList<Thread>();

		for (int i = 0; i < threadNum; i++) {
			log.debug("Start working thread " + i);
			WorkerThread workerThread = new WorkerThread();
			workerThread.setName("SubVF2Th-" + i);
			workerThread.start();
			threadsPool.add(workerThread);
		}

		try {
			for (Thread thread : threadsPool) {
				thread.join();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		log.info("results size = " + results.size());

		isomorphics.addAll(results);
		return isomorphics;
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean isSubgraphIsomorphic(Graph<V1, E1> g1, int v1, Graph<V2, E2> g2, int v2) {

		State state = makeInitialState(g1, v1, g2, v2);
		return match(state);
	}

	/**
	 * Creates an empty {@link State} for mapping the vertices of {@code g1} to
	 * {@code g2}.
	 */
	private State makeInitialState(Graph<V1, E1> g1, int v1, Graph<V2, E2> g2, int v2) {
		return new SubVF2State<V1, E1, V2, E2>(g1, v1, g2, v2);
	}

	public int getThreadNumLimit() {
		return threadNumLimit;
	}

	public void setThreadNumLimit(int threadNumLimit) {
		this.threadNumLimit = threadNumLimit;
	}

	public int getThreadTimeoutLimit() {
		return threadTimeoutLimit;
	}

	public void setThreadTimeoutLimit(int threadTimeoutLimit) {
		this.threadTimeoutLimit = threadTimeoutLimit;
	}

	private class WorkerThread extends Thread {

		@Override
		public void run() {

			try {
				while (!cands.isEmpty()) {
					int can = cands.poll();
					// log.debug("current candidate is " + can);
					if (isSubgraphIsomorphic(g1, du, g2, can)) {
						results.add(can);
					}
				}
			} catch (RuntimeException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	private boolean match(State s) {
		if (s.isGoal())
			return true;

		if (s.isDead()) {
			return false;
		}

		int n1 = NULL_NODE, n2 = NULL_NODE;
		Pair<Integer> next = null;
		boolean found = false;
		while (!found && (next = s.nextPair(n1, n2)) != null) {
			n1 = next.x;
			n2 = next.y;
			if (s.isFeasiblePair(n1, n2)) {
				State copy = s.copy();
				copy.addPair(n1, n2);
				found = match(copy);
				// If we found a match, then don't bother backtracking as it
				// would be wasted effort.
				if (!found)
					copy.backTrack();
			}
		}
		return found;
	}

}
