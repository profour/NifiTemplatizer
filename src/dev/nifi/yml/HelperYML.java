package dev.nifi.yml;

import org.apache.nifi.api.toolkit.model.PositionDTO;

public class HelperYML {
	// Enumeration of statically available types for elements on the workspace
	public enum ReservedComponents {
		INPUT_PORT,
		OUTPUT_PORT,
		PROCESS_GROUP, 
		REMOTE_PROCESS_GROUP,
		FUNNEL, 
		LABEL
	}
	
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
	
	
	public static PositionDTO createPosition(String pos) {

		PositionDTO position = new PositionDTO();
		String[] parts = pos.split(",");
		position.setX(new Double(parts[0]));
		position.setY(new Double(parts[1]));
		
		return position;
	}
}
