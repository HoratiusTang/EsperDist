package dist.esper.core.util;


public class Options {
	public static String LOG4J_CONF_PATH="log4j.conf";
	public static String CONFIG_FILE_PATH="conf";
	public static String COORDINATOR_ID="coordinator.id";
	public static String COORDINATOR_IP="coordinator.ip";
	public static String COORDINATOR_PORT="coordinator.port";
	
	public static String WORKER_NUMBER_OF_PROCESS_THREADS="worker.nprct";
	public static String WORKER_NUMBER_OF_PUBLISH_THREADS="worker.npubt";
	
	public static String THIS_ID="this.id";
	public static String THIS_PORT="this.port";
	
	public static String EVENT_CATEGORY="event.category";
	public static String EVENT_NAME="event.name";
	
	public static String QUERY_FILE="qf";
	public static String LOG_QUERY_RESULT="lqr";//"true" or "false"
	
	public static String KRYONET_WRITE_BUFFER_SIZE="kryonet.wbs";
	public static String KRYONET_OBJECT_BUFFER_SIZE="kryonet.obs";
	
	public static String OUTPUT_INTERVERAL_US="outintus";
	public static String SPOUT_BATCH_COUNT="spout.bc";
	
	public static String EXPECTED_SPOUNT_COUNT="spout.count";
}
