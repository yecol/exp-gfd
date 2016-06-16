package inf.ed.graph.structure;

public interface Edge {

	/**
	 * Returns {@code true} if matches.
	 */
	boolean match(Object o);

	/**
	 * Returns the index of the tail vertex
	 */
	Vertex from();

	/**
	 * Returns the index of the head vertex
	 */
	Vertex to();

}
