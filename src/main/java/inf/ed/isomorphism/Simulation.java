package inf.ed.isomorphism;

import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.adaptor.TypedEdge;
import inf.ed.graph.structure.adaptor.VertexOString;
import inf.ed.graph.structure.adaptor.VertexString;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

public class Simulation {

	static public boolean match(Graph<VertexOString, OrthogonalEdge> ball,
			Graph<VertexString, TypedEdge> Q) {

		Int2ObjectMap<IntSet> sim = new Int2ObjectOpenHashMap<IntSet>();
		// mapping between nodes and their matches

		for (VertexString vq : Q.allVertices().values()) {

			if (!sim.containsKey(vq.getID())) {
				sim.put(vq.getID(), new IntOpenHashSet());
			}
			IntSet matches = sim.get(vq.getID());
			for (VertexOString vg : ball.allVertices().values()) {
				if (vq.match(vg)) {
					matches.add(vg.getID());
				}
			}
			sim.put(vq.getID(), matches);
		}

		Queue<Integer> q = new LinkedList<Integer>();
		HashSet<Integer> rdtfumat = new HashSet<Integer>();
		HashSet<Integer> rdttumat = new HashSet<Integer>();
		/**
		 * detect those nodes whose match set varies, then initialises queue q
		 */
		for (TypedEdge te : Q.allEdges()) {
			int fu = te.from().getID();
			int tu = te.to().getID();
			IntSet fumat = sim.get(fu);
			IntSet tumat = sim.get(tu);

			for (int fv : fumat) {
				boolean ismatch = false;
				for (int tv : tumat) {
					if (ball.contains(fv, tv)) {
						ismatch = true;
						break;
					}
				}
				if (!ismatch) {
					rdtfumat.add(fv);
					if (!q.contains(fu)) { // can be improved
						q.add(fu);
					}
				}
			}
			fumat.removeAll(rdtfumat);
			if (fumat.isEmpty()) {
				sim.clear();
				// return sim;
				return false;
			}
			rdtfumat.clear();

			for (int tv : tumat) {
				boolean ismatch = false;
				for (int fv : fumat) {
					if (ball.contains(fv, tv)) {
						if (ball.getEdge(fv, tv).match(te)) {
							ismatch = true;
							break;
						}
					}
				}
				if (!ismatch) {
					rdttumat.add(tv);
					if (!q.contains(tu)) { // can be improved
						q.add(tu);
					}
				}
			}
			tumat.removeAll(rdttumat);
			if (tumat.isEmpty()) {
				sim.clear();
				// return sim;
				return false;
			}
			rdttumat.clear();
		}

		HashSet<Integer> temp = new HashSet<Integer>(); // used to maintain
														// invalid matches
		while (!q.isEmpty()) {
			int u = q.poll();
			IntSet umat = sim.get(u);
			temp.clear();
			for (int v : umat) {
				boolean ismatch = true;
				for (int tu : Q.getChildren(u)) {
					IntSet tumat = sim.get(tu);
					boolean flag = false;
					for (int tv : tumat) {
						if (ball.contains(v, tv)) {
							if (ball.getEdge(v, tv).match(Q.getEdge(u, tu))) {
								flag = true;
								break;
							}
						}
					}
					if (!flag) {
						ismatch = false;
						break;
					}
				}
				if (!ismatch) {
					temp.add(v);
				}

				/**
				 * if v satisfies requirements in the downward direction, then
				 * we test whether it meets the conditions in the upward
				 * direction.
				 * */
				if (ismatch) {
					for (int fu : Q.getParents(u)) {
						IntSet fumat = sim.get(fu);
						boolean flag = false;
						for (int fv : fumat) {
							if (ball.contains(fv, v)) {
								if (ball.getEdge(fv, v).match(Q.getEdge(fu, u))) {
									flag = true;
									break;
								}
							}
						}
						if (!flag) {
							ismatch = false;
							break;
						}
					}
					if (!ismatch) {
						temp.add(v);
					}
				}
			}

			/**
			 * if there are changes---some nodes should be removed from umat as
			 * they are invliad then we remove nodes in temp and push u's
			 * neighbours onto q.
			 */
			if (!temp.isEmpty()) {
				umat.removeAll(temp);
				if (!umat.isEmpty()) {
					for (int tu : Q.getChildren(u)) {
						if (!q.contains(tu)) {
							q.add(tu);
						}
					}
					for (int fu : Q.getParents(u)) {
						if (!q.contains(fu)) {
							q.add(fu);
						}
					}
				} else {
					sim.clear();
					break;
				}
			}
		}

		return sim.isEmpty() ? false : true;
	}
}
