package inf.ed.graph.structure;

import java.util.Map;

public class EdgeFactory<E> {

	private final Class<E> edgeClass;

	public EdgeFactory(Class<E> edgeClass) {
		this.edgeClass = edgeClass;
	}

	public E createEdge() {
		try {
			return this.edgeClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Edge factory failed", e);
		}
	}

	public E createEdgeWithMap(Map<String, Object> o) {
		try {
			return this.edgeClass.getDeclaredConstructor(Map.class).newInstance(o);
		} catch (Exception e) {
			throw new RuntimeException("Edge factory with string failed", e);
		}
	}

	public E createEdgeWithString(String s) {
		try {
			return this.edgeClass.getDeclaredConstructor(String.class).newInstance(s);
		} catch (Exception e) {
			throw new RuntimeException("Edge factory with string failed", e);
		}
	}

}
