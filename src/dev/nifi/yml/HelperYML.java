package dev.nifi.yml;

import org.apache.nifi.api.toolkit.model.PositionDTO;
import org.apache.nifi.api.toolkit.model.ConnectionDTO.LoadBalanceCompressionEnum;
import org.apache.nifi.api.toolkit.model.ConnectionDTO.LoadBalanceStrategyEnum;

public class HelperYML {
	// Enumeration of statically available types for elements on the workspace
	public enum ReservedComponents {
		INPUT_PORT,
		OUTPUT_PORT,
		PROCESS_GROUP, 
		REMOTE_PROCESS_GROUP,
		FUNNEL, 
		LABEL;
		
		public boolean isType(String type) {
			return name().equalsIgnoreCase(type);
		}
	}
	
	public static String YAML_EXT = ".yaml";
	
	// font-size
	public static String DEFAULT_STYLE_FONT_SIZE = "12px";
	
	// background-color
	public static String DEFAULT_STYLE_COLOR = "#fff7d7";

	// Constants for style names
	public static final String WIDTH  = "width";
	public static final String HEIGHT = "height";
	public static final String BG_COLOR = "background-color";
	public static final String FONT_SIZE = "font-size";
	
	// Remote Process Group Defaults
	public static final String DEFAULT_REMOTE_TRANSPORT = "RAW";
	public static final String DEFAULT_REMOTE_TIMEOUT = "30 sec";
	public static final String DEFAULT_REMOTE_YIELD = "10 sec";
	
	// Input Connection special property names
	public static final String NAME = "name";
	public static final String BACK_PRESSURE_OBJECT_THRESHOLD= "backPressureObjectThreshold";
	public static final String BACK_PRESSURE_DATA_SIZE_THRESHOLD = "backPressureDataSizeThreshold";
	public static final String LOAD_BALANCE_STRATEGY = "loadBalanceStrategy";
	public static final String LOAD_BALANCE_PARTITION_ATTRIBUTE = "loadBalancePartitionAttribute";
	public static final String LOAD_BALANCE_COMPRESSION = "loadBalanceCompression";
	public static final String FLOW_FILE_EXPIRATION = "flowFileExpiration";
	public static final String PRIORITIZERS = "prioritizers";
	
	// Default values for input connection properties
	public static final String DEFAULT_FLOW_FILE_EXPIRATION = "0 sec";
	public static final String DEFAULT_BACK_PRESSURE_DATA_SIZE_THRESHOLD = "1 GB";
	public static final int DEFAULT_BACK_PRESSURE_OBJECT_THRESHOLD = 10000;
	public static final LoadBalanceStrategyEnum DEFAULT_LOAD_BALANCE_STRATEGY = LoadBalanceStrategyEnum.DO_NOT_LOAD_BALANCE;
	public static final LoadBalanceCompressionEnum DEFAULT_LOAD_BALANCE_COMPRESSION = LoadBalanceCompressionEnum.DO_NOT_COMPRESS;
	
	// Remote Process Group special property names
	public static final String TARGET_URIS = "targetUris";
	public static final String PROXY_HOST = "proxyHost";
	public static final String PROXY_PORT = "proxyPort";
	public static final String PROXY_USER = "proxyUser";
	public static final String PROXY_PASSWORD = "proxyPassword";
	public static final String NETWORK = "network";
	public static final String PROTOCOL = "protocol";
	public static final String TIMEOUT = "timeout";
	public static final String YIELD_DURATION = "yieldDuration";
	
	public static PositionDTO createPosition(String pos) {

		PositionDTO position = new PositionDTO();
		String[] parts = pos.split(",");
		position.setX(new Double(parts[0]));
		position.setY(new Double(parts[1]));
		
		return position;
	}
	
	public static String formatDoubleTruncated(Double d) {
		return String.format("%.0f", d);
	}
	
	public static String formatPosition(Double x, Double y) {
		return String.format("%.0f,%.0f", x, y);
	}
	
	public static boolean isProcessGroup(String type) {
		return HelperYML.ReservedComponents.PROCESS_GROUP.isType(type) || 
			   HelperYML.ReservedComponents.REMOTE_PROCESS_GROUP.isType(type);
	}
	
	public static boolean isPort(String type) {
		return HelperYML.ReservedComponents.INPUT_PORT.isType(type) ||
			   HelperYML.ReservedComponents.OUTPUT_PORT.isType(type);
	}
}
