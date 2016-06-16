package inf.ed.isomorphism;

import static inf.ed.isomorphism.State.NULL_NODE;
import inf.ed.gfd.structure.GFD2;
import inf.ed.gfd.structure.WorkUnit;
import inf.ed.gfd.util.KV;
import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.adaptor.Pair;
import inf.ed.graph.structure.adaptor.TypedEdge;
import inf.ed.graph.structure.adaptor.VertexOString;
import inf.ed.graph.structure.adaptor.VertexString;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LocalViolationEnumInspector {

	static Logger log = LogManager.getLogger(LocalViolationEnumInspector.class);

	ConcurrentLinkedQueue<WorkUnit> workload = new ConcurrentLinkedQueue<WorkUnit>();
	ConcurrentLinkedQueue<Int2IntMap> matches = new ConcurrentLinkedQueue<Int2IntMap>();

	Graph<VertexOString, OrthogonalEdge> fragment;

	int threadNumLimit = 0;

	public LocalViolationEnumInspector(Collection<WorkUnit> workunits,
			Graph<VertexOString, OrthogonalEdge> fragment) {
		this.threadNumLimit = KV.ISOMORPHISM_THREAD_LIMIT;
		this.workload.addAll(workunits);
		this.fragment = fragment;
		log.info("local violation isomorphism test begin:" + this.workload.size());
	}

	public List<Int2IntMap> findIsomorphic() {
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
		log.info("find matches finished.");
		return new LinkedList<Int2IntMap>(matches);
	}

	/**
	 * {@inheritDoc}
	 */
	public boolean isSubgraphIsomorphic(GFD2 gfd, Graph<VertexString, TypedEdge> g1, int v1,
			Graph<VertexOString, OrthogonalEdge> g2, int v2) {

		State state = makeInitialState(g1, v1, g2, v2);
		return match(state, gfd);
	}

	/**
	 * Creates an empty {@link State} for mapping the vertices of {@code g1} to
	 * {@code g2}.
	 */
	private State makeInitialState(Graph<VertexString, TypedEdge> g1, int v1,
			Graph<VertexOString, OrthogonalEdge> g2, int v2) {
		return new SubVF2State<VertexString, TypedEdge, VertexOString, OrthogonalEdge>(g1, v1, g2,
				v2);
	}

	private class WorkerThread extends Thread {
		@Override
		public void run() {

			try {
				while (!workload.isEmpty()) {
					if (workload.size() % 100 == 0) {
						log.info("current workload size = " + workload.size());
					}
					WorkUnit wu = workload.poll();

					if (!fragment.contains(wu.candidate1)) {
						log.error("wu.candidate1:" + wu.candidate1 + " is not in this partition");
					}

					else {
						isSubgraphIsomorphic(wu.gfd, wu.gfd.getPattern(), wu.pivot1, fragment,
								wu.candidate1);
					}
				}
				log.info("i claim finish.");
			} catch (RuntimeException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	private boolean match(State s, GFD2 gfd) {

		if (s.isGoal()) {
			// log.info("find a match:" + s.getMatch().toString());
			Int2IntMap match = new Int2IntOpenHashMap(s.getMatch());
			if (gfd.isViolation(match, fragment)) {
				matches.add(match);
			}
			return true;
		}

		if (s.isDead()) {
			return false;
		}

		int n1 = NULL_NODE, n2 = NULL_NODE;
		Pair<Integer> next = null;
		boolean found = false;
		while ((next = s.nextPair(n1, n2)) != null) {
			n1 = next.x;
			n2 = next.y;
			if (s.isFeasiblePair(n1, n2)) {
				State copy = s.copy();
				copy.addPair(n1, n2);
				found = match(copy, gfd);
				// If we found a match, then don't bother backtracking as it
				// would be wasted effort.
				// if (!found)
				copy.backTrack();
			}
		}
		return found;
	}

}
