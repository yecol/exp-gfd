package inf.ed.gfd.util;

public class KV {

	/** coordinator service name */
	public static final String COORDINATOR_SERVICE_NAME = "grape-coordinator";

	public static final String ANY = "ANY";
	public static final int ATTR_LIMIT = 3;
	public static final int ALL_CANDIDATES_LIMIT = 1000;
	public static final int FREQ_EDGE_LIMIT = 150;

	public static final int XCONDITION = 0;
	public static final int YCONDITION = 1;

	public static final String CONDITION_TYPE_EQUAL_LET = "eq-let";
	public static final String CONDITION_TYPE_EQUAL_VAR = "eq-var";
	public static final String CONDITION_TYPE_HAS_PROP = "has-prop";

	public static final String PTN_INPUT_FLAG_CCNUM = "%CCNum";
	public static final String PTN_INPUT_FLAG_PTN1 = "%P1";
	public static final String PTN_INPUT_FLAG_PTN2 = "%P2";
	public static final String PTN_INPUT_FLAG_CONX = "%X";
	public static final String PTN_INPUT_FLAG_CONY = "%Y";
	public static final String PTN_INPUT_FLAG_CAND = "%Cand";

	public static final String DATASET_POKEC = "POKEC";
	public static final String DATASET_YAGO = "YAGO";
	public static final String DATASET_DBPEDIA = "DBPEDIA";

	public static final String SETTING_FRAGMENT = "fragmentedG";
	public static final String SETTING_REPLICATE = "replicatedG";

	/** coordinator RMI service port */
	public static int RMI_PORT = 1099;

	public static String GRAPH_FILE_PATH = null;
	public static String QUERY_DIR_PATH = null;
	public static String OUTPUT_DIR = "";

	public static String DATASET = "";

	public static int ISOMORPHISM_THREAD_LIMIT = Integer.MAX_VALUE;
	public static int CANDIDATES_LIMIT = Integer.MAX_VALUE;
	public static int NODE_AS_TARGET_EDGE_LIMIT = Integer.MAX_VALUE;

	/** load constant from properties file */
	static {
		try {
			RMI_PORT = Config.getInstance().getIntProperty("RMI_PORT");

			GRAPH_FILE_PATH = Config.getInstance().getStringProperty("GRAPH_FILE_PATH");

			QUERY_DIR_PATH = Config.getInstance().getStringProperty("QUERY_DIR_PATH");

			OUTPUT_DIR = Config.getInstance().getStringProperty("OUTPUT_DIR");

			ISOMORPHISM_THREAD_LIMIT = Config.getInstance().getIntProperty(
					"ISOMORPHISM_THREAD_LIMIT");
			ISOMORPHISM_THREAD_LIMIT = (ISOMORPHISM_THREAD_LIMIT == 0 ? Integer.MAX_VALUE
					: ISOMORPHISM_THREAD_LIMIT);

			CANDIDATES_LIMIT = Config.getInstance().getIntProperty("CANDIDATES_LIMIT");
			CANDIDATES_LIMIT = (CANDIDATES_LIMIT == 0 ? Integer.MAX_VALUE : CANDIDATES_LIMIT);

			NODE_AS_TARGET_EDGE_LIMIT = Config.getInstance().getIntProperty(
					"NODE_AS_TARGET_EDGE_LIMIT");
			NODE_AS_TARGET_EDGE_LIMIT = (NODE_AS_TARGET_EDGE_LIMIT == 0 ? Integer.MAX_VALUE
					: NODE_AS_TARGET_EDGE_LIMIT);

			DATASET = Config.getInstance().determineDataset();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
