package inf.ed.grape.interfaces;

import java.io.Serializable;
import java.util.Collection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class Result implements Serializable {

	static Logger log = LogManager.getLogger(Result.class);

	/** a function how to assemble partial results to a final result. */
	public abstract void assemblePartialResults(
			Collection<Result> partialResults);

	/** a function how write file results to a final result. */
	public abstract void writeToFile(String filename);
}
