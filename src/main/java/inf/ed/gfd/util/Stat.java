package inf.ed.gfd.util;

public class Stat {

	private static Stat instance = null;

	// read graph files.
	public double totalTime = 0.0;

	public String setting;

	// read graph files.
	public double getInputFilesLocalAndDistributedTime = 0.0;

	// job assignment time.
	public double jobAssignmentTime = 0.0;

	// local detect time.
	public double localDetectViolationTime = 0.0;

	// communication cost.
	public double communicationData = 0.0;

	// communication cost.
	public double crossingEdgeData = 0.0;

	// finish gap between first and last
	public double finishGapTime = 0.0;

	// find all the candidates time
	public double findCandidatesTime = 0.0;

	public int totalGfdCount = 0;
	public int totalViolation = 0;
	public int totalWorkUnit = 0;

	protected Stat() {
	}

	public static synchronized Stat getInstance() {
		if (instance == null) {
			instance = new Stat();
		}
		return instance;
	}

	public static double df(double number) {
		number = Math.round(number * 1000);
		number = number / 1000;
		return number;
	}

	public String getInfo() {

		String RUN_MODE = "";
		if (Params.RUN_MODE == Params.VAR_BASE) {
			RUN_MODE = "base";
		}
		if (Params.RUN_MODE == Params.VAR_RANDOM) {
			RUN_MODE = "random";
		}
		if (Params.RUN_MODE == Params.VAR_OPT) {
			RUN_MODE = "optimized";
		}

		String ret = "PLOT_DATA ********************************************\n";
		ret += "PLOT_DATA description: " + setting + ", " + KV.DATASET + ", n="
				+ Params.N_PROCESSORS + ", run_mode=" + RUN_MODE + "\n";
		ret += "PLOT_DATA totalTime: " + df(totalTime) + "s.\n";
		ret += "PLOT_DATA readInputTime: " + df(getInputFilesLocalAndDistributedTime) + "s.\n";
		ret += "PLOT_DATA findCandidateTime: " + df(findCandidatesTime) + "s.\n";
		ret += "PLOT_DATA jobAssignmentTime: " + df(jobAssignmentTime) + "s.\n";
		ret += "PLOT_DATA localDetectTime: " + df(localDetectViolationTime) + "s.\n";
		ret += "PLOT_DATA finishGap: " + df(finishGapTime) + "s.\n";
		ret += "PLOT_DATA crossingedges: " + df(crossingEdgeData / (1024 * 1024)) + "M.\n";
		ret += "PLOT_DATA datashipment: " + df(communicationData / (1024 * 1024)) + "M.\n";
		ret += "PLOT_DATA totalViolation/GFD/workunit: " + totalViolation + "/" + totalGfdCount
				+ "/" + totalWorkUnit;

		return ret;
	}

}
