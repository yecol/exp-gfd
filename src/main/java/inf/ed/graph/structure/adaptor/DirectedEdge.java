package inf.ed.graph.structure.adaptor;

import inf.ed.graph.structure.Edge;
import inf.ed.graph.structure.Vertex;

import java.io.Serializable;

public class DirectedEdge implements Edge, Serializable {

	/**
	 * Directed edge.
	 */
	private static final long serialVersionUID = 1L;
	Vertex from;
	Vertex to;
	String attr;

	public DirectedEdge(Object from, Object to) {
		this.from = (Vertex) from;
		this.to = (Vertex) to;
	}

	public DirectedEdge(Vertex from, Vertex to) {
		this.from = from;
		this.to = to;
	}

	public boolean match(Object o) {
		return true;
	}

	public Vertex from() {
		return from;
	}

	public Vertex to() {
		return to;
	}

	public String toString() {
		return "dEdge [f=" + from.getID() + ", t=" + to.getID() + "]";
	}

}