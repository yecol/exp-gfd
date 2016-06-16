package inf.ed.graph.structure;

import java.io.Serializable;

import org.jgrapht.EdgeFactory;

public class SimpleEdgeFactory<V, E> implements EdgeFactory<V, E>, Serializable {

	private static final long serialVersionUID = 1L;
	private final Class<E> edgeClass;

	public SimpleEdgeFactory(Class<E> edgeClass) {
		this.edgeClass = edgeClass;
	}

	public E createEdge(V sourceVertex, V targetVertex) {
		try {
			return this.edgeClass.getDeclaredConstructor(Object.class, Object.class).newInstance(
					sourceVertex, targetVertex);
		} catch (Exception e) {
			throw new RuntimeException("Edge factory with object failed", e);
		}
	}

}
