package inf.ed.gfd.detect.dist;

import inf.ed.grape.interfaces.Result;
import it.unimi.dsi.fastutil.ints.Int2IntMap;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class ViolationResult extends Result implements Serializable {

	private static final long serialVersionUID = 1L;
	List<Int2IntMap> violations;

	public List<Int2IntMap> getViolations() {
		return this.violations;
	}

	public void addViolations(List<Int2IntMap> violations) {
		if (this.violations == null) {
			this.violations = new LinkedList<Int2IntMap>();
		}
		this.violations.addAll(violations);
	}

	@Override
	public void assemblePartialResults(Collection<Result> partialResults) {

		if (this.violations == null) {
			this.violations = new LinkedList<Int2IntMap>();
		}

		for (Result r : partialResults) {
			ViolationResult vr = (ViolationResult) r;
			if (vr.getViolations() != null) {
				this.violations.addAll(vr.getViolations());
			}
		}
	}

	@Override
	public void writeToFile(String filename) {

		System.out.println("Write final result file to:" + filename);

		PrintWriter writer;
		try {
			writer = new PrintWriter(filename);
			for (Int2IntMap vio : violations) {
				writer.println(vio);
			}
			writer.flush();
			writer.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
}
