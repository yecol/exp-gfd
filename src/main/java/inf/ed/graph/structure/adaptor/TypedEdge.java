package inf.ed.graph.structure.adaptor;

import inf.ed.gfd.util.KV;
import inf.ed.graph.structure.Edge;
import inf.ed.graph.structure.Vertex;

import java.io.Serializable;

public class TypedEdge implements Edge, Serializable {

	/**
	 * Directed edge.
	 */
	private static final long serialVersionUID = 1L;
	Vertex from;
	Vertex to;
	private int[] attrs = { -1, -1, -1 };
	int attrCount = 0;

	public TypedEdge(Object from, Object to) {
		this.from = (Vertex) from;
		this.to = (Vertex) to;
	}

	public TypedEdge(Vertex from, Vertex to) {
		this.from = from;
		this.to = to;
	}

	public boolean match(Object o) {
		System.out.println("need to check");
		return true;
	}

	public Vertex from() {
		return from;
	}

	public Vertex to() {
		return to;
	}

	public int[] getAttr() {
		return this.attrs;
	}

	public String getAttrString() {
		String s = "[";
		for (int i = 0; i < attrCount; i++) {
			s += attrs[i] + ",";
		}
		return s + "]";
	}

	public void setAttr(int attr) {
		if (attrCount < KV.ATTR_LIMIT) {
			this.attrs[attrCount] = attr;
			attrCount++;
		}
	}

	@Override
	public String toString() {
		return "dEdge [f=" + from.getID() + ", t=" + to.getID() + ", attr=" + getAttrString()
				+ " ]";
	}

}