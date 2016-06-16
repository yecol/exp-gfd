package inf.ed.graph.structure;

import inf.ed.gfd.structure.Ball;
import inf.ed.graph.structure.adaptor.TypedEdge;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.SimpleDirectedGraph;

public class SimpleGraph<V extends Vertex, E extends Edge> implements Graph<V, E>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private DirectedGraph<V, E> graph;
	private VertexFactory<V> vertexFactory;
	private Int2ObjectMap<V> vertices;
	static Logger log = LogManager.getLogger(SimpleGraph.class);

	public SimpleGraph(Class<V> vertexClass, Class<E> edgeClass) {
		vertexFactory = new VertexFactory<V>(vertexClass);
		SimpleEdgeFactory<V, E> ef = new SimpleEdgeFactory<V, E>(edgeClass);
		graph = new SimpleDirectedGraph<V, E>(ef);
	}

	public boolean addVertex(V vertex) {
		if (vertices != null) {
			vertices.put(vertex.getID(), vertex);
		}
		return graph.addVertex(vertex);
	}

	@SuppressWarnings("unchecked")
	public boolean addEdge(E edge) {
		return graph.addEdge((V) edge.from(), (V) edge.to(), edge);
	}

	public E addEdge(V from, V to) {
		return graph.addEdge(from, to);
	}

	@SuppressWarnings("unchecked")
	public Set<V> getChildren(V vertex) {

		Set<V> children = new HashSet<V>();
		for (E e : graph.outgoingEdgesOf(vertex)) {
			children.add((V) e.to());
		}
		return children;
	}

	@SuppressWarnings("unchecked")
	public Set<V> getParents(V vertex) {

		Set<V> parents = new HashSet<V>();
		for (E e : graph.incomingEdgesOf(vertex)) {
			parents.add((V) e.from());
		}
		return parents;
	}

	@SuppressWarnings("unchecked")
	public Set<V> getNeighbours(V vertex) {

		Set<V> neighbours = new HashSet<V>();
		for (E e : graph.incomingEdgesOf(vertex)) {
			neighbours.add((V) e.from());
		}
		for (E e : graph.outgoingEdgesOf(vertex)) {
			neighbours.add((V) e.to());
		}
		return neighbours;
	}

	public Set<Integer> getChildren(int vertexID) {
		Set<Integer> children = new HashSet<Integer>();
		for (E e : graph.outgoingEdgesOf(this.allVertices().get(vertexID))) {
			children.add(e.to().getID());
		}
		return children;
	}

	public Set<Integer> getParents(int vertexID) {

		Set<Integer> parents = new HashSet<Integer>();
		for (E e : graph.incomingEdgesOf(this.allVertices().get(vertexID))) {
			parents.add(e.from().getID());
		}
		return parents;
	}

	public Set<Integer> getNeighbours(int vertexID) {
		Set<Integer> neighbours = new HashSet<Integer>();
		for (E e : graph.incomingEdgesOf(this.allVertices().get(vertexID))) {
			neighbours.add(e.from().getID());
		}
		for (E e : graph.outgoingEdgesOf(this.allVertices().get(vertexID))) {
			neighbours.add(e.to().getID());
		}
		return neighbours;
	}

	public int edgeSize() {
		return graph.edgeSet().size();
	}

	public int vertexSize() {
		return graph.vertexSet().size();
	}

	public V getVertex(int vID) {
		return this.allVertices().get(vID);
	}

	public V getRandomVertex() {
		return null;
	}

	public Set<E> allEdges() {
		return graph.edgeSet();
	}

	public Int2ObjectMap<V> allVertices() {

		if (vertices == null) {
			vertices = new Int2ObjectOpenHashMap<V>();
			for (V vertex : graph.vertexSet()) {
				vertices.put(vertex.getID(), vertex);
			}
		}

		return vertices;
	}

	public void clear() {
		throw new IllegalArgumentException("Simple graph doesn't support this method currently.");
	}

	public void clearEdges() {
		throw new IllegalArgumentException("Simple graph doesn't support this method currently.");

	}

	public boolean contains(int vertexID) {
		return this.allVertices().containsKey(vertexID);
	}

	public boolean contains(V from, V to) {
		return graph.containsEdge(from, to);
	}

	public boolean contains(E edge) {
		return graph.containsEdge(edge);
	}

	public int degree(int vertexID) {
		V vertex = this.getVertex(vertexID);
		return this.graph.inDegreeOf(vertex) + this.graph.outDegreeOf(vertex);
	}

	public Set<E> getEdges(V vertex1, V vertex2) {
		return graph.getAllEdges(vertex1, vertex2);
	}

	public boolean hasCycles() {
		throw new IllegalArgumentException("Simple graph doesn't support this method currently.");
	}

	public boolean removeEdge(E edge) {
		return graph.removeEdge(edge);
	}

	public boolean removeEdge(V from, V to) {
		graph.removeEdge(from, to);
		return true;
	}

	public boolean removeVertex(V vertex) {
		if (vertices != null) {
			vertices.remove(vertex.getID());
		}
		return graph.removeVertex(vertex);
	}

	public void finalizeGraph() {
	}

	public boolean loadGraphFromVEFile(String filePathWithoutExtension, boolean isTypedEdge) {
		FileInputStream fileInputStream = null;
		Scanner sc = null;

		try {

			fileInputStream = new FileInputStream(filePathWithoutExtension);

			sc = new Scanner(fileInputStream, "UTF-8");
			while (sc.hasNextLine()) {

				String line = sc.nextLine();

				if (line.startsWith("#")) {
					// statistics
				} else if (line.startsWith("v")) {

					// add vertex
					V v = vertexFactory.createVertexWithString(line);
					this.addVertex(v);
				}

				else if (line.startsWith("e")) {
					// add edge

					String[] elements = line.split("\t");

					V source = this.getVertex(Integer.parseInt(elements[1].trim()));
					V target = this.getVertex(Integer.parseInt(elements[3].trim()));

					E e = this.addEdge(source, target);
					if (isTypedEdge) {
						TypedEdge te = (TypedEdge) e;
						te.setAttr(Integer.parseInt(elements[2].trim()));
					}
				}

			}

			if (fileInputStream != null) {
				fileInputStream.close();
			}
			if (sc != null) {
				sc.close();
			}

			log.info("query graph loaded.");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return true;
	}

	public boolean contains(int fromID, int toID) {
		return graph.containsEdge(this.getVertex(fromID), this.getVertex(toID));
	}

	@SuppressWarnings("unchecked")
	public Graph<V, E> getSubgraph(Class<V> vertexClass, Class<E> edgeClass, V center, int bound) {

		Graph<V, E> subgraph = new SimpleGraph<V, E>(vertexClass, edgeClass);

		Queue<V> q = new LinkedList<V>();
		Int2IntMap visited = new Int2IntOpenHashMap();
		q.add(center);
		visited.put(center.getID(), 0);
		subgraph.addVertex(center);

		while (!q.isEmpty()) {
			V current = q.poll();
			int dist = visited.get(current.getID());
			if (dist >= bound) {
				break;
			}
			for (E e : graph.outgoingEdgesOf(current)) {
				V tv = (V) e.to();
				if (!visited.keySet().contains(tv.getID())) {
					q.add(tv);
					visited.put(tv.getID(), dist + 1);
					if (!subgraph.contains(tv.getID())) {
						subgraph.addVertex(tv);
					}
				}
				if (!subgraph.contains(current, tv)) {
					subgraph.addEdge(current, tv);
				}
			}
			for (E e : graph.incomingEdgesOf(current)) {
				V fv = (V) e.from();
				if (!visited.keySet().contains(fv.getID())) {
					q.add(fv);
					visited.put(fv.getID(), dist + 1);
					if (!subgraph.contains(fv.getID())) {
						subgraph.addVertex(fv);
					}
				}
				if (!subgraph.contains(fv, current)) {
					subgraph.addEdge(fv, current);
				}
			}
		}
		return subgraph;
	}

	public void display(int limit) {
		System.out.println("The graph has the following structure: ");
		System.out.println(graph.vertexSet().size() + " vertices -  limit " + limit);
		int i = 0;
		for (V v : graph.vertexSet()) {
			if (i > limit) {
				break;
			} else {
				i++;
				System.out.println(v);
			}
		}
		i = 0;
		System.out.println(graph.edgeSet().size() + " edges -  limit " + limit);
		for (E e : graph.edgeSet()) {
			if (i > limit) {
				break;
			} else {
				i++;
				System.out.println(e);
			}
		}
	}

	public int getRadius(V center) {
		int max = 0;
		HashMap<V, Integer> visited = new HashMap<V, Integer>();
		Queue<V> q = new LinkedList<V>();
		q.add(center);
		visited.put(center, 0);
		int distance = 0;
		while (!q.isEmpty()) {
			V vertex = q.poll();
			distance = visited.get(vertex);
			if (distance > max) {
				max = distance;
			}
			for (E e : graph.outgoingEdgesOf(vertex)) {
				V tv = (V) e.to();
				if (!visited.keySet().contains(tv)) {
					q.add(tv);
					visited.put(tv, distance + 1);
				}
			}
			for (E e : graph.incomingEdgesOf(vertex)) {
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

		for (V vf : graph.vertexSet()) {
			visited.clear();
			q.clear();
			q.add(vf);
			visited.put(vf.getID(), 0);
			while (!q.isEmpty()) {
				V v = q.poll();
				int dist = visited.get(v.getID());
				for (E e : graph.outgoingEdgesOf(v)) {
					V tv = (V) e.to();
					if (!visited.keySet().contains(tv.getID())) {
						q.add(tv);
						visited.put(tv.getID(), dist + 1);
					}
				}
				for (E e : graph.incomingEdgesOf(v)) {
					V fv = (V) e.from();
					if (!visited.keySet().contains(fv.getID())) {
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

	public E getEdge(V vertex1, V vertex2) {
		return this.graph.getEdge(vertex1, vertex2);
	}

	public E getEdge(int fromID, int toID) {
		return this.graph.getEdge(this.getVertex(fromID), this.getVertex(toID));
	}

	@Override
	public int getRadius(int vertexID) {
		V vertex = this.vertices.get(vertexID);
		return getRadius(vertex);
	}

	@Override
	public int getBallSize(int centerID, int r) {
		return -1;
	}

	@Override
	public Ball getBall(int centerID, int r) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Graph<V, E> getSubgraph(Class<V> vertexClass, Class<E> edgeClass, V center, int bound,
			IntSet labelSet) {
		// TODO Auto-generated method stub
		return null;
	}

}
