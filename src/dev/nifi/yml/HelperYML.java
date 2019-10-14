package dev.nifi.yml;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.api.toolkit.model.PositionDTO;
import org.apache.nifi.api.toolkit.model.ConnectionDTO.LoadBalanceCompressionEnum;
import org.apache.nifi.api.toolkit.model.ConnectionDTO.LoadBalanceStrategyEnum;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

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
	
	// Processor special property names
	public static final String SCHEDULING_PERIOD = "runSchedule";
	public static final String SCHEDULING_STRATEGY = "schedulingStrategy";
	public static final String SCHEDULABLE_TASK_COUNT = "concurrentTasks";
	public static final String PENALTY_DURATION = "penaltyDuration";
	public static final String YIELD_DURATION = "yieldDuration";
	public static final String RUN_DURATION = "runDuration";
	public static final String EXECUTION_NODE = "execution";
	public static final String BULLETIN_LEVEL = "bulletinLevel";
	
	// Processor Special Property Defaults
	public static final String DEFAULT_SCHEDULING_PERIOD = "0 sec";
	public static final String DEFAULT_SCHEDULING_STRATEGY = "TIMER_DRIVEN";
	public static final int DEFAULT_SCHEDULABLE_TASK_COUNT = 1;
	public static final String DEFAULT_PENALTY_DURATION = "30 sec";
	public static final String DEFAULT_YIELD_DURATION = "1 sec";
	public static final long DEFAULT_RUN_DURATION = 0L;
	public static final String DEFAULT_EXECUTION_NODE = "ALL";
	public static final String DEFAULT_BULLETIN_LEVEL = "WARN";
	
	// Remote Process Group Defaults
	public static final String DEFAULT_REMOTE_TRANSPORT = "RAW";
	public static final String DEFAULT_REMOTE_TIMEOUT = "30 sec";
	public static final String DEFAULT_REMOTE_YIELD = "10 sec";
	
	// Input Connection special property names
	public static final String NAME = "name";
	public static final String Z_INDEX = "zIndex";
	public static final String LABEL_INDEX = "labelIndex";
	public static final String BACK_PRESSURE_OBJECT_THRESHOLD= "backPressureObjectThreshold";
	public static final String BACK_PRESSURE_DATA_SIZE_THRESHOLD = "backPressureDataSizeThreshold";
	public static final String LOAD_BALANCE_STRATEGY = "loadBalanceStrategy";
	public static final String LOAD_BALANCE_PARTITION_ATTRIBUTE = "loadBalancePartitionAttribute";
	public static final String LOAD_BALANCE_COMPRESSION = "loadBalanceCompression";
	public static final String FLOW_FILE_EXPIRATION = "flowFileExpiration";
	public static final String PRIORITIZERS = "prioritizers";
	
	// Default values for input connection properties
	public static final String DEFAULT_FLOW_FILE_EXPIRATION_SEC = "0 sec";
	public static final String DEFAULT_FLOW_FILE_EXPIRATION_MINS = "0 mins";
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
	// REPEATED (commented out here for reference purposes)
	// public static final String YIELD_DURATION = "yieldDuration";
	
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
	
	public static boolean isProcessor(String type) {
		for (ReservedComponents c : ReservedComponents.values()) {
			if (c.isType(type)) {
				return false;
			}
		}
		// If it isn't a reserved type, it must be a processor
		return true;
	}
	
	public static List<TemplateYML> load(final String importDir) throws JsonParseException, JsonMappingException, IOException {
		List<TemplateYML> templates = new ArrayList<TemplateYML>();
		
		YAMLFactory f = new YAMLFactory();
		ObjectMapper mapper = new ObjectMapper(f);
		
		File templateDir = new File(importDir);
		
		for (String templateName : templateDir.list()) {
			if (templateName.endsWith(HelperYML.YAML_EXT)) {
				File template = new File(templateDir.getAbsolutePath() + File.separator + templateName);
				TemplateYML yml = mapper.readValue(template, TemplateYML.class);
				templates.add(yml);
			}
		}
		
		return templates;
	}
	
	public static void export(String outputDir, List<TemplateYML> templates) throws IOException {
		YAMLFactory f = new YAMLFactory();
		f.enable(YAMLGenerator.Feature.MINIMIZE_QUOTES);
		f.disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);
		ObjectMapper mapper = new ObjectMapper(f);
		
		mapper.setSerializationInclusion(Include.NON_EMPTY);
		
		for (TemplateYML template : templates) {
			String yaml = mapper.writer().writeValueAsString(template);
			
			// Formatting to make it easier to read the templates
			yaml = yaml.replaceAll("\n-", "\n\n-");
			yaml = yaml.replace("\ndependencies:", "\n\ndependencies:");
			yaml = yaml.replace("\ncontrollers:", "\n\ncontrollers:");
			yaml = yaml.replace("\ncontrollers:\n", "\ncontrollers:");
			yaml = yaml.replace("\ncomponents:", "\n\ncomponents:");
			yaml = yaml.replace("\ncomponents:\n", "\ncomponents:");
			
			try (FileWriter writer = new FileWriter(outputDir + File.separator + template.name + HelperYML.YAML_EXT)) {
				writer.write(yaml);
			}
		}
	}
}
