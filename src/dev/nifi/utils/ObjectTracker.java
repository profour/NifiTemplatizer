package dev.nifi.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ObjectTracker {

	// GroupId -> Name -> TrackedObjects
	private final Map<String, Map<String, Set<TrackedObject>>> groupedNameMap = new HashMap<>();
	
	private final Map<String, String> idMapping = new HashMap<String, String>();
	
	public String lookupByOldId(String oldId) {
		return idMapping.get(oldId);
	}
	
	public String getIdForObject(String groupId, String name, String type) {
		Map<String, Set<TrackedObject>> namedObjects = groupedNameMap.get(groupId);
		if (namedObjects != null) {
			Set<TrackedObject> objectsWithName = namedObjects.get(name);
			
			if (objectsWithName != null) {
				List<TrackedObject> matches = new ArrayList<TrackedObject>();
				for (TrackedObject obj : objectsWithName) {
					if (type.equals(obj.type)) {
						matches.add(obj);
					}
				}
				
				if (!matches.isEmpty()) {
					if (matches.size() > 1) {
						throw new IllegalStateException("Found more than one matching TrackedObject for search: " +
									groupId + " " + name + " " + type);	
					}
					
					return matches.get(0).id;
				}
				
			}
		}
		
		return null;
	}
	
	public void track(String groupId, String name, String type, String id) {
		if (!groupedNameMap.containsKey(groupId)) {
			groupedNameMap.put(groupId, new HashMap<>());
		}
		
		Map<String, Set<TrackedObject>> namedObjects = groupedNameMap.get(groupId);
		if (!namedObjects.containsKey(name)) {
			namedObjects.put(name, new HashSet<>());
		}
		
		Set<TrackedObject> objectsWithName = namedObjects.get(name);
		
		objectsWithName.add(new TrackedObject(type, id));
	}
	
	public void track(String oldId, String newId) {
		idMapping.put(oldId, newId);
	}
	
	private final class TrackedObject {
		public final String type;
		public final String id;
		
		public TrackedObject(String type, String id) {
			this.type = type;
			this.id = id;
		}
		
		public boolean equals(Object o) {
			return (o instanceof TrackedObject) && ((TrackedObject)o).id.equals(this.id);
		}
		
		public int hashCode() {
			return id.hashCode();
		}
	}
}
