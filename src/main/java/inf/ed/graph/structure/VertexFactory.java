package inf.ed.graph.structure;

import java.io.Serializable;
import java.util.Map;

public class VertexFactory<V> implements Serializable {

	private static final long serialVersionUID = 1L;
	private final Class<V> vertexClass;

	public VertexFactory(Class<V> vertexClass) {
		this.vertexClass = vertexClass;
	}

	public V createVertex() {
		try {
			return this.vertexClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Vertex factory failed", e);
		}
	}

	public V createVertexWithMap(Map<String, Object> o) {
		try {
			return this.vertexClass.getDeclaredConstructor(Map.class, String.class).newInstance(o,
					"v");
		} catch (Exception e) {
			throw new RuntimeException("Vertex factory with object failed", e);
		}
	}

	public V createVertexWithString(String s) {
		try {
			return this.vertexClass.getDeclaredConstructor(String.class).newInstance(s);
		} catch (Exception e) {
			throw new RuntimeException("Vertex factory with string failed", e);
		}
	}
}
