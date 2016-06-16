package inf.ed.graph.structure;

import inf.ed.gfd.structure.Ball;
import inf.ed.gfd.structure.TransEdge;
import inf.ed.gfd.util.KV;
import inf.ed.graph.structure.adaptor.VertexOString;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * OrthogonalGraph (十字链表法) Ref:http://blog.fishc.com/2535.html
 * 
 * @author Xin Wang
 */
public class OrthogonalGraph<V extends OrthogonalVertex>
		implements Graph<V, OrthogonalEdge>, Serializable {

	static Logger log = LogManager.getLogger(OrthogonalGraph.class);

	private static final long serialVersionUID = 1L;

	private Int2ObjectMap<V> vertices;
	private int edgeSize; // amount of edges
	private VertexFactory<V> vertexFactory;
	int ballCenter = -1;

	public OrthogonalGraph(Class<V> vertexClass) {
		// this.vertices = new TIntObjectHashMap<V>();
		this.vertices = new Int2ObjectOpenHashMap<V>();
		this.vertexFactory = new VertexFactory<V>(vertexClass);
	}

	public boolean addVertex(V vertex) {
		this.vertices.put(vertex.getID(), vertex);
		return true;
	}

	public OrthogonalEdge addEdge(V from, V to) {
		OrthogonalEdge e = new OrthogonalEdge();
		e.SetFromNode(from);
		e.SetToNode(to);
		OrthogonalEdge hlink = to.GetFirstIn();
		OrthogonalEdge tlink = from.GetFirstOut();
		e.SetHLink(hlink);
		e.SetTLink(tlink);
		from.SetFirstOut(e);
		to.SetFirstIn(e);
		this.edgeSize++;
		return e;
	}

	public int edgeSize() {
		return this.edgeSize;
	}

	public int vertexSize() {
		return this.vertices.size();
	}

	public boolean contains(int vertexID) {
		return this.vertices.containsKey(vertexID);
	}

	public boolean removeEdge(V fv, V tv) {
		OrthogonalEdge e1 = fv.GetFirstOut();
		if (e1.to().equals(tv)) {
			fv.SetFirstOut(e1.GetTLink());
		} else {
			for (OrthogonalEdge e = e1; e != null; e = e.GetTLink()) {
				OrthogonalEdge ne = e.GetTLink();
				if (ne != null) {
					if (ne.to().equals(tv)) {
						e.SetTLink(ne.GetTLink());
						break;
					}
				}
			}
		}

		e1 = tv.GetFirstIn();
		if (e1.from().equals(fv)) {
			tv.SetFirstIn(e1.GetHLink());
		} else {
			for (OrthogonalEdge e = e1; e != null; e = e.GetHLink()) {
				OrthogonalEdge ne = e.GetHLink();
				if (ne != null) {
					if (ne.from().equals(fv)) {
						e.SetHLink(ne.GetHLink());
						break;
					}
				}
			}
		}
		this.edgeSize -= 1;
		return true;
	}

	@SuppressWarnings("unchecked")
	public boolean removeVertex(V vertex) {
		this.vertices.remove(vertex.getID());
		// TODO 需要调整this.edgeSize
		// FIXME 需要调整this.edgeSize
		// process n's parents
		OrthogonalEdge ep = vertex.GetFirstIn();
		if (ep != null) {
			for (OrthogonalEdge e = ep; e != null; e = e.GetHLink()) {
				V fv = (V) e.from();
				for (OrthogonalEdge e1 = fv.GetFirstOut(); e1 != null; e1 = e1.GetTLink()) {
					OrthogonalEdge e2 = e1.GetTLink(); // e2.GetToNode() 有可能是n
					V cfv = (V) e1.to();
					if (cfv.equals(vertex)) { // e1.firstout 指向了被删除的边
						fv.SetFirstOut(e2); // 如果e1.tonode = n,
											// 则设置fv的firstout为e2
						break;
					} else {
						V cfv2 = (V) e2.to();
						if (cfv2.equals(vertex)) {
							e1.SetTLink(e2.GetTLink()); // e2是e1的邻边,
														// 如果e2.tonode = n
														// (说明e2为被删除的边),
														// 则设置e1的tlink为e2的tlink
							break;
						}
					}
				}
			}
		}

		// process n's children
		ep = vertex.GetFirstOut();
		if (ep != null) {
			for (OrthogonalEdge e = ep; e != null; e = e.GetTLink()) {
				V tv = (V) e.to();
				for (OrthogonalEdge e1 = tv.GetFirstIn(); e1 != null; e1 = e1.GetHLink()) {
					OrthogonalEdge e2 = e1.GetHLink();
					V ptv = (V) e1.from();
					if (ptv.equals(vertex)) {
						tv.SetFirstIn(e2);
						break;
					} else {
						V ptv2 = (V) e2.from();
						if (ptv2.equals(vertex)) {
							e1.SetHLink(e2.GetHLink());
							break;
						}
					}
				}
			}
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	public Set<V> getChildren(V vertex) {
		HashSet<V> cSet = new HashSet<V>();
		for (OrthogonalEdge e = vertex.GetFirstOut(); e != null; e = e.GetTLink()) {
			V child = (V) e.to();
			cSet.add(child);
		}
		return cSet;
	}

	@SuppressWarnings("unchecked")
	public Set<V> getParents(V vertex) {
		HashSet<V> pSet = new HashSet<V>();
		for (OrthogonalEdge e = vertex.GetFirstIn(); e != null; e = e.GetHLink()) {
			V parent = (V) e.from();
			pSet.add(parent);
		}
		return pSet;
	}

	public Set<V> getNeighbours(V vertex) {
		Set<V> neighbours = this.getChildren(vertex);
		neighbours.addAll(this.getParents(vertex));
		return neighbours;
	}

	@SuppressWarnings("unchecked")
	public Set<Integer> getChildren(int vertexID) {
		HashSet<Integer> cSet = new HashSet<Integer>();
		OrthogonalEdge e1 = this.getVertex(vertexID).GetFirstOut();
		for (OrthogonalEdge e = e1; e != null; e = e.GetTLink()) {
			V child = (V) e.to();
			cSet.add(child.getID());
		}
		return cSet;
	}

	@SuppressWarnings("unchecked")
	public Set<Integer> getParents(int vertexID) {
		HashSet<Integer> pSet = new HashSet<Integer>();
		OrthogonalEdge e1 = this.getVertex(vertexID).GetFirstIn();
		for (OrthogonalEdge e = e1; e != null; e = e.GetHLink()) {
			V parent = (V) e.from();
			pSet.add(parent.getID());
		}
		return pSet;
	}

	public Set<Integer> getNeighbours(int vertexID) {
		Set<Integer> neighbours = this.getChildren(vertexID);
		neighbours.addAll(this.getParents(vertexID));
		return neighbours;
	}

	public V getVertex(int vID) {
		return this.vertices.get(vID);
	}

	public Int2ObjectMap<V> allVertices() {
		return this.vertices;
	}

	public V getRandomVertex() {
		// pick a random vertex should not call in a large graph
		assert this.vertexSize() < 1000;
		// using TIntObjectHashMap
		// int size = this.vertices.keys().length;
		// Random r = new Random();
		// int randomID = this.vertices.keys()[r.nextInt(size)];
		// return this.vertices.get(randomID);
		int item = new Random().nextInt(this.vertexSize());
		int i = 0;
		for (V v : this.vertices.values()) {
			if (i == item)
				return v;
			i = i + 1;
		}

		return null;
	}

	public boolean contains(V from, V to) {
		Queue<OrthogonalEdge> q = new LinkedList<OrthogonalEdge>();
		OrthogonalEdge e = from.GetFirstOut();
		if (e == null) {
			return false;
		} else {
			q.add(e);
		}

		while (!q.isEmpty()) {
			OrthogonalEdge ee = q.poll();
			if (ee.to() == to) {
				return true;
			}
			OrthogonalEdge ne = ee.GetTLink();
			if (ne != null) {
				q.add(ne);
			}
		}
		return false;
	}

	public int degree(int vertexID) {
		int dg = 0;
		OrthogonalEdge e1 = this.getVertex(vertexID).GetFirstOut();
		for (OrthogonalEdge e = e1; e != null; e = e.GetTLink()) {
			// count children
			dg++;
		}
		e1 = this.getVertex(vertexID).GetFirstIn();
		for (OrthogonalEdge e = e1; e != null; e = e.GetHLink()) {
			// count parent
			dg++;
		}
		return dg;
	}

	public boolean addEdge(OrthogonalEdge edge) {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	public boolean contains(OrthogonalEdge edge) {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	public boolean removeEdge(OrthogonalEdge edge) {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	public Set<OrthogonalEdge> getEdges(V vertex1, V vertex2) {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	public void clear() {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	public void clearEdges() {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	public boolean hasCycles() {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	public Set<OrthogonalEdge> allEdges() {
		if (ballCenter == -1) {
			throw new IllegalArgumentException(
					"all edges not support without assigned a ball center.");
		} else {
			Set<OrthogonalEdge> edgeSet = new ObjectOpenHashSet<OrthogonalEdge>();
			for (OrthogonalEdge e = this.getVertex(ballCenter).GetFirstIn(); e != null; e = e
					.GetHLink()) {
				edgeSet.add(e);
			}

			for (OrthogonalEdge e = this.getVertex(ballCenter).GetFirstOut(); e != null; e = e
					.GetTLink()) {
				edgeSet.add(e);
			}
			return edgeSet;
		}
	}

	public void finalizeGraph() {
		return;
	}

	public boolean loadGraphFromVEFile(String filePathWithoutExtension, boolean isTypedEdge) {

		try {
			File vertexFile = new File(filePathWithoutExtension + ".v");
			File edgeFile = new File(filePathWithoutExtension + ".e");

			long start = System.currentTimeMillis();
			LineIterator it = FileUtils.lineIterator(vertexFile, "UTF-8");
			try {
				while (it.hasNext()) {
					String line = it.nextLine();
					V v = vertexFactory.createVertexWithString(line);
					this.addVertex(v);
				}
			} finally {
				LineIterator.closeQuietly(it);
			}
			log.info("graph nodes loaded. with size = " + this.vertexSize() + ",using = "
					+ (System.currentTimeMillis() - start));

			it = FileUtils.lineIterator(edgeFile, "UTF-8");
			int ln = 0;
			start = System.currentTimeMillis();
			try {
				while (it.hasNext()) {
					ln++;
					if (ln % 100000 == 0) {
						log.debug("processed " + ln + " lines");
					}
					String[] elements = it.nextLine().split("\t");
					try {
						V source = this.getVertex(Integer.parseInt(elements[0].trim()));
						V target = this.getVertex(Integer.parseInt(elements[2].trim()));

						if (!this.contains(source, target)) {
							this.addEdge(source, target);
						}
						this.getEdge(source, target).setAttr(Integer.parseInt(elements[1].trim()));

					} catch (Exception e) {
					}
				}
			} finally {
				LineIterator.closeQuietly(it);
			}
			log.info("graph edge loaded. with size = " + this.edgeSize() + ",using = "
					+ (System.currentTimeMillis() - start));
			log.info("graph loaded.");
			return true;

		} catch (IOException e) {
			log.error("load graph file failed.");
			e.printStackTrace();
		}
		return false;
	}

	public boolean contains(int fromID, int toID) {
		V from = this.vertices.get(fromID);
		Queue<OrthogonalEdge> q = new LinkedList<OrthogonalEdge>();
		OrthogonalEdge e = from.GetFirstOut();
		if (e == null) {
			return false;
		} else {
			q.add(e);
		}

		while (!q.isEmpty()) {
			OrthogonalEdge ee = q.poll();
			if (ee.to().getID() == toID) {
				return true;
			}
			OrthogonalEdge ne = ee.GetTLink();
			if (ne != null) {
				q.add(ne);
			}
		}
		return false;
	}

	@Override
	public int getBallSize(int centerID, int r) {
		int size = 0;
		Int2IntMap visited = new Int2IntOpenHashMap();
		Queue<Integer> q = new LinkedList<Integer>();
		q.add(centerID);
		visited.put(centerID, 0);
		OrthogonalEdge e = null;
		int distance = 0;
		while (!q.isEmpty()) {
			int vertexID = q.poll();
			size++;
			distance = visited.get(vertexID);
			if (distance >= r) {
				continue;
			}

			for (e = this.getVertex(vertexID).GetFirstOut(); e != null; e = e.GetTLink()) {
				int tv = e.to().getID();
				if (!visited.keySet().contains(tv)) {
					q.add(tv);
					visited.put(tv, distance + 1);
				}
			}

			int parentCount = 0;
			for (e = this.getVertex(vertexID).GetFirstIn(); e != null; e = e.GetHLink()) {
				parentCount++;
				if (parentCount > KV.NODE_AS_TARGET_EDGE_LIMIT) {
					// log.debug("vertex:" + vertexID + " has more than "
					// + KV.NODE_AS_TARGET_EDGE_LIMIT + " parents, break;");
					break;
				}
				int fv = e.from().getID();
				if (!visited.keySet().contains(fv)) {
					q.add(fv);
					visited.put(fv, distance + 1);
				}
			}
		}
		return size;
	}

	@Override
	public Ball getBall(int center, int bound) {
		// TODO Auto-generated method stub
		if (!this.vertices.containsKey(center)) {
			log.error("this partition didnot contains vertex:" + center);
			return null;
		}

		Ball b = new Ball(center, bound);
		b.center = center;
		VertexOString centerV = (VertexOString) this.vertices.get(center);
		HashMap<Integer, Integer> visited = new HashMap<Integer, Integer>();
		Queue<VertexOString> q = new LinkedList<VertexOString>();
		q.add(centerV);
		visited.put(center, 0);
		b.vertices.put(center, centerV.getAttr());
		OrthogonalEdge e = null;
		int distance = 0;
		while (!q.isEmpty()) {
			VertexOString vertex = q.poll();
			distance = visited.get(vertex.getID());
			if (distance == bound) {
				for (e = vertex.GetFirstOut(); e != null; e = e.GetTLink()) {
					if (b.vertices.containsKey(e.to().getID())) {
						for (int i = 0; i < e.attrCount; i++) {
							b.edges.add(
									new TransEdge(vertex.getID(), e.getAttr()[i], e.to().getID()));
						}
					}
				}
				for (e = vertex.GetFirstIn(); e != null; e = e.GetHLink()) {
					if (b.vertices.containsKey(e.from().getID())) {
						for (int i = 0; i < e.attrCount; i++) {
							b.edges.add(new TransEdge(e.from().getID(), e.getAttr()[i],
									vertex.getID()));
						}
					}
				}
				continue;
			}

			for (e = vertex.GetFirstOut(); e != null; e = e.GetTLink()) {
				VertexOString tv = (VertexOString) e.to();
				if (!visited.keySet().contains(tv.getID())) {
					q.add(tv);
					b.vertices.put(tv.getID(), tv.getAttr());
					visited.put(tv.getID(), distance + 1);
				}
				for (int i = 0; i < e.attrCount; i++) {
					b.edges.add(new TransEdge(vertex.getID(), e.getAttr()[i], tv.getID()));
				}
			}

			// get all parents.
			int parentCount = 0;
			for (e = vertex.GetFirstIn(); e != null; e = e.GetHLink()) {
				parentCount++;
				if (parentCount > KV.NODE_AS_TARGET_EDGE_LIMIT) {
					// log.debug("vertex:" + vertex.getID() + " has more than "
					// + KV.NODE_AS_TARGET_EDGE_LIMIT + " parents, break;");
					break;
				}
				VertexOString fv = (VertexOString) e.from();
				if (!visited.keySet().contains(fv.getID())) {
					q.add(fv);
					b.vertices.put(fv.getID(), fv.getAttr());
					visited.put(fv.getID(), distance + 1);
				}
				for (int i = 0; i < e.attrCount; i++) {
					b.edges.add(new TransEdge(fv.getID(), e.getAttr()[i], vertex.getID()));
				}
			}

		}
		return b;

	}

	@SuppressWarnings("unchecked")
	public Graph<V, OrthogonalEdge> getSubgraph(Class<V> vertexClass,
			Class<OrthogonalEdge> edgeClass, V center, int bound) {
		OrthogonalGraph<V> subgraph = new OrthogonalGraph<V>(vertexClass);
		subgraph.ballCenter = center.getID();

		HashMap<V, Integer> visited = new HashMap<V, Integer>();
		Queue<V> q = new LinkedList<V>();
		q.add(center);
		V cv = (V) center.copyWithoutEdge();
		visited.put(cv, 0);
		subgraph.addVertex(cv);
		OrthogonalEdge e = null;
		int distance = 0;
		while (!q.isEmpty()) {
			V vertex = q.poll();
			V cvcopy = subgraph.getVertex(vertex.getID());
			distance = visited.get(cvcopy);
			if (distance == bound) {
				OrthogonalEdge e1 = this.getVertex(vertex.getID()).GetFirstOut();
				for (e = e1; e != null; e = e.GetTLink()) {
					int tvID = e.to().getID();
					if (subgraph.allVertices().keySet().contains(tvID)
							&& !subgraph.contains(vertex.getID(), tvID)) {
						V tv = subgraph.getVertex(tvID);
						subgraph.addEdge(cvcopy, tv);
						subgraph.getEdge(cvcopy, tv).copyAttr(e);
					}
				}

				e1 = vertex.GetFirstIn();
				for (e = e1; e != null; e = e.GetHLink()) {
					int fvID = e.from().getID();
					if (subgraph.allVertices().keySet().contains(fvID)
							&& !subgraph.contains(fvID, vertex.getID())) {
						V fv = subgraph.getVertex(fvID);
						subgraph.addEdge(fv, cvcopy);
						subgraph.getEdge(fv, cvcopy).copyAttr(e);
					}
				}
				continue;
			}

			for (e = vertex.GetFirstOut(); e != null; e = e.GetTLink()) {
				V tv = (V) e.to();
				V tvcopy = (V) tv.copyWithoutEdge();
				if (!visited.keySet().contains(tv)) {
					q.add(tv);
					subgraph.addVertex(tvcopy);
					visited.put(tvcopy, distance + 1);
				}
				subgraph.addEdge(cvcopy, tvcopy);
				subgraph.getEdge(cvcopy, tvcopy).copyAttr(e);
			}

			// get all parents.
			int parentCount = 0;
			for (e = vertex.GetFirstIn(); e != null; e = e.GetHLink()) {
				parentCount++;
				if (parentCount > KV.NODE_AS_TARGET_EDGE_LIMIT) {
					// log.debug("vertex:" + vertex.getID() + " has more than "
					// + KV.NODE_AS_TARGET_EDGE_LIMIT + " parents, break;");
					break;
				}
				V fv = (V) e.from();
				V fvcopy = (V) fv.copyWithoutEdge();
				if (!visited.keySet().contains(fv)) {
					q.add(fv);
					subgraph.addVertex(fvcopy);
					visited.put(fvcopy, distance + 1);
				}
				subgraph.addEdge(fvcopy, cvcopy);
				subgraph.getEdge(fvcopy, cvcopy).copyAttr(e);
			}

		}
		return subgraph;
	}

	public void display(int limit) {
		System.out.println("The graph has the following structure: ");
		int i = 0;
		for (V n : this.allVertices().values()) {
			if (i < limit) {
				OrthogonalEdge e1 = n.GetFirstOut();
				String s = "";
				if (e1 != null) {
					for (OrthogonalEdge e = e1; e != null; e = e.GetTLink()) {
						s = s + ", -" + e.getAttrString() + "->" + e.to().getID();
					}
					if (!s.equals(""))
						s = s.substring(2);
				}
				System.out.println(n.getID() + ", links: " + s);
				i++;
			} else
				break;
		}
	}

	@SuppressWarnings("unchecked")
	public int getRadius(V center) {
		int max = 0;
		HashMap<V, Integer> visited = new HashMap<V, Integer>();
		Queue<V> q = new LinkedList<V>();
		q.add(center);
		visited.put(center, 0);
		OrthogonalEdge e = null;
		int distance = 0;
		while (!q.isEmpty()) {
			V vertex = q.poll();
			distance = visited.get(vertex);
			if (distance > max) {
				max = distance;
			}

			OrthogonalEdge e1 = vertex.GetFirstOut();
			for (e = e1; e != null; e = e.GetTLink()) {
				V tv = (V) e.to();
				if (!visited.keySet().contains(tv)) {
					q.add(tv);
					visited.put(tv, distance + 1);
				}
			}

			e1 = vertex.GetFirstIn();
			for (e = e1; e != null; e = e.GetHLink()) {
				V fv = (V) e.from();
				if (!visited.keySet().contains(fv)) {
					q.add(fv);
					visited.put(fv, distance + 1);
				}
			}
		}
		return max;
	}

	@SuppressWarnings("unchecked")
	public int getDiameter() {
		int max = 0;
		Int2IntMap visited = new Int2IntOpenHashMap();
		Queue<V> q = new LinkedList<V>();

		for (V vf : vertices.values()) {
			visited.clear();
			q.clear();
			q.add(vf);
			visited.put(vf.getID(), 0);
			while (!q.isEmpty()) {
				V v = q.poll();
				int dist = visited.get(v);
				OrthogonalEdge e1 = this.getVertex(v.getID()).GetFirstOut();
				for (OrthogonalEdge e = e1; e != null; e = e.GetTLink()) {
					V tv = (V) e.to();
					if (!visited.keySet().contains(tv.getID())) {
						q.add(tv);
						visited.put(tv.getID(), dist + 1);
					}
				}
				e1 = this.getVertex(v.getID()).GetFirstIn();
				for (OrthogonalEdge e = e1; e != null; e = e.GetHLink()) {
					V fv = (V) e.from();
					if (!visited.keySet().contains(fv)) {
						q.add(fv);
						visited.put(fv.getID(), dist + 1);
					}
				}
			}

			for (int v : visited.keySet()) {
				int dist = visited.get(v);
				if (dist > max) {
					max = dist;
				}
			}
		}
		return max;
	}

	public OrthogonalEdge getEdge(V vertex1, V vertex2) {
		for (OrthogonalEdge e = vertex1.GetFirstOut(); e != null; e = e.GetTLink()) {
			if (e.to().equals(vertex2)) {
				return e;
			}
		}
		return null;
	}

	public OrthogonalEdge getEdge(int fromID, int toID) {
		V v1 = this.vertices.get(fromID);
		V v2 = this.vertices.get(toID);
		for (OrthogonalEdge e = v1.GetFirstOut(); e != null; e = e.GetTLink()) {
			if (e.to().equals(v2)) {
				return e;
			}
		}
		return null;
	}

	@Override
	public int getRadius(int vertexID) {
		V vertex = this.vertices.get(vertexID);
		return getRadius(vertex);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Graph<V, OrthogonalEdge> getSubgraph(Class<V> vertexClass,
			Class<OrthogonalEdge> edgeClass, V center, int bound, IntSet labelSet) {
		OrthogonalGraph<V> subgraph = new OrthogonalGraph<V>(vertexClass);
		subgraph.ballCenter = center.getID();

		HashMap<V, Integer> visited = new HashMap<V, Integer>();
		Queue<V> q = new LinkedList<V>();
		q.add(center);
		V cv = (V) center.copyWithoutEdge();
		visited.put(cv, 0);
		subgraph.addVertex(cv);
		OrthogonalEdge e = null;
		int distance = 0;
		while (!q.isEmpty()) {
			V vertex = q.poll();
			V cvcopy = subgraph.getVertex(vertex.getID());
			distance = visited.get(cvcopy);
			if (distance == bound) {
				for (e = vertex.GetFirstOut(); e != null; e = e.GetTLink()) {
					if (e.hasPossibleMatch(labelSet)) {
						int tvID = e.to().getID();
						if (subgraph.allVertices().keySet().contains(tvID)
								&& !subgraph.contains(vertex.getID(), tvID)) {
							V tv = subgraph.getVertex(tvID);
							subgraph.addEdge(cvcopy, tv);
							subgraph.getEdge(cvcopy, tv).copyAttr(e);
						}
					}
				}
				for (e = vertex.GetFirstIn(); e != null; e = e.GetHLink()) {
					if (e.hasPossibleMatch(labelSet)) {
						int fvID = e.from().getID();
						if (subgraph.allVertices().keySet().contains(fvID)
								&& !subgraph.contains(fvID, vertex.getID())) {
							V fv = subgraph.getVertex(fvID);
							subgraph.addEdge(fv, cvcopy);
							subgraph.getEdge(fv, cvcopy).copyAttr(e);
						}
					}
				}
				continue;
			}

			for (e = vertex.GetFirstOut(); e != null; e = e.GetTLink()) {
				if (e.hasPossibleMatch(labelSet)) {
					V tv = (V) e.to();
					V tvcopy = (V) tv.copyWithoutEdge();
					if (!visited.keySet().contains(tv)) {
						q.add(tv);
						subgraph.addVertex(tvcopy);
						visited.put(tvcopy, distance + 1);
					}
					subgraph.addEdge(cvcopy, tvcopy);
					subgraph.getEdge(cvcopy, tvcopy).copyAttr(e);
				}
			}

			// get all parents.
			int parentCount = 0;
			for (e = vertex.GetFirstIn(); e != null; e = e.GetHLink()) {
				if (e.hasPossibleMatch(labelSet)) {
					parentCount++;
					if (parentCount > KV.NODE_AS_TARGET_EDGE_LIMIT) {
						break;
					}
					V fv = (V) e.from();
					V fvcopy = (V) fv.copyWithoutEdge();
					if (!visited.keySet().contains(fv)) {
						q.add(fv);
						subgraph.addVertex(fvcopy);
						visited.put(fvcopy, distance + 1);
					}
					subgraph.addEdge(fvcopy, cvcopy);
					subgraph.getEdge(fvcopy, cvcopy).copyAttr(e);
				}
			}

		}
		return subgraph;
	}
}