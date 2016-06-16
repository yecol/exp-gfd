package inf.ed.gfd.util;


public class Dev {

	private static final long MEGABYTE = 1024L * 1024L;

	public static String currentRuntimeState() {
		// Get the Java runtime
		Runtime runtime = Runtime.getRuntime();
		// Run the garbage collector
		runtime.gc();
		// Calculate the used memory
		long memory = runtime.totalMemory() - runtime.freeMemory();
		return "Used memory is (m): " + memory / MEGABYTE;
	}
}
