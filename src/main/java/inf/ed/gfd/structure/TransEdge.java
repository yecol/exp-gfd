package inf.ed.gfd.structure;

import java.io.Serializable;

public class TransEdge implements Serializable {

	private static final long serialVersionUID = 1L;
	public int from;
	public int to;
	public int type;

	public TransEdge(int from, int type, int to) {
		this.from = from;
		this.type = type;
		this.to = to;
	}

	public String toString() {
		return (from + "-" + type + "->" + to);
	}

	@Override
	public int hashCode() { // *** note capitalization of the "C"
		final int prime = 31;
		int result = 1;
		result = prime * result + from;
		result = prime * result + type;
		result = prime * result + to;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		TransEdge other = (TransEdge) obj;
		return this.from == other.from && this.type == other.type && this.to == other.to;
	}

}
