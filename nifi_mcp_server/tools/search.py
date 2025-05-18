from typing import Dict, Any, List, Optional, Union
from ..nifi_api import NiFiAPIClient
from loguru import logger

# Search Tools

async def search_components(nifi_client: NiFiAPIClient, search_term: str, 
                         component_types: List[str] = None, 
                         pg_id: str = "root", 
                         recursive: bool = True) -> Dict[str, Any]:
    """Search for components by name or type.
    
    Args:
        nifi_client: NiFi API client instance
        search_term: Term to search for
        component_types: List of component types to search (processors, connections, process_groups)
        pg_id: ID of the parent process group (default: root)
        recursive: Whether to search recursively in child process groups
        
    Returns:
        Dictionary with matching components
    """
    try:
        search_term_lower = search_term.lower()
        
        # Default to all component types if none specified
        if not component_types:
            component_types = ["processors", "process_groups", "connections", "input_ports", "output_ports"]
        
        # Initialize result structure
        result = {
            "status": "success",
            "search_term": search_term,
            "matches": {
                "processors": [],
                "process_groups": [],
                "connections": [],
                "input_ports": [],
                "output_ports": [],
                "remote_process_groups": [],
                "parameter_contexts": [],
                "templates": []
            },
            "total_matches": 0
        }
        
        # Get all components in the current process group
        response = nifi_client.get(f"/flow/process-groups/{pg_id}")
        flow = response.get("processGroupFlow", {}).get("flow", {})
        
        # Search processors
        if "processors" in component_types:
            processors = flow.get("processors", [])
            for processor in processors:
                name = processor.get("name", "").lower()
                processor_type = processor.get("component", {}).get("type", "").lower()
                
                if search_term_lower in name or search_term_lower in processor_type:
                    result["matches"]["processors"].append({
                        "id": processor.get("id"),
                        "name": processor.get("name"),
                        "type": processor.get("component", {}).get("type"),
                        "pg_id": pg_id,
                        "state": processor.get("status", {}).get("runStatus")
                    })
        
        # Search process groups
        if "process_groups" in component_types:
            process_groups = flow.get("processGroups", [])
            for pg in process_groups:
                name = pg.get("name", "").lower()
                comments = pg.get("comments", "").lower()
                
                if search_term_lower in name or search_term_lower in comments:
                    result["matches"]["process_groups"].append({
                        "id": pg.get("id"),
                        "name": pg.get("name"),
                        "comments": pg.get("comments"),
                        "parent_id": pg_id
                    })
        
        # Search connections
        if "connections" in component_types:
            connections = flow.get("connections", [])
            for conn in connections:
                name = conn.get("name", "").lower()
                source_name = conn.get("sourceGroupName", "").lower() + "." + conn.get("sourceConnectable", {}).get("name", "").lower()
                dest_name = conn.get("destinationGroupName", "").lower() + "." + conn.get("destinationConnectable", {}).get("name", "").lower()
                
                if (search_term_lower in name or 
                    search_term_lower in source_name or 
                    search_term_lower in dest_name):
                    result["matches"]["connections"].append({
                        "id": conn.get("id"),
                        "name": conn.get("name"),
                        "source": {
                            "id": conn.get("sourceId"),
                            "name": conn.get("sourceConnectable", {}).get("name"),
                            "type": conn.get("sourceType"),
                            "group_name": conn.get("sourceGroupName")
                        },
                        "destination": {
                            "id": conn.get("destinationId"),
                            "name": conn.get("destinationConnectable", {}).get("name"),
                            "type": conn.get("destinationType"),
                            "group_name": conn.get("destinationGroupName")
                        },
                        "pg_id": pg_id
                    })
        
        # Search input ports
        if "input_ports" in component_types:
            input_ports = flow.get("inputPorts", [])
            for port in input_ports:
                name = port.get("name", "").lower()
                
                if search_term_lower in name:
                    result["matches"]["input_ports"].append({
                        "id": port.get("id"),
                        "name": port.get("name"),
                        "state": port.get("status", {}).get("runStatus"),
                        "pg_id": pg_id
                    })
        
        # Search output ports
        if "output_ports" in component_types:
            output_ports = flow.get("outputPorts", [])
            for port in output_ports:
                name = port.get("name", "").lower()
                
                if search_term_lower in name:
                    result["matches"]["output_ports"].append({
                        "id": port.get("id"),
                        "name": port.get("name"),
                        "state": port.get("status", {}).get("runStatus"),
                        "pg_id": pg_id
                    })
        
        # If recursive, also search in child process groups
        if recursive:
            process_groups = flow.get("processGroups", [])
            
            for child_group in process_groups:
                child_id = child_group.get("id")
                child_results = await search_components(
                    nifi_client, 
                    search_term, 
                    component_types, 
                    child_id, 
                    recursive
                )
                
                if child_results.get("status") == "success":
                    # Merge the child results with the parent results
                    for comp_type, matches in child_results.get("matches", {}).items():
                        result["matches"][comp_type].extend(matches)
        
        # Calculate total number of matches
        total = sum(len(matches) for matches in result["matches"].values())
        result["total_matches"] = total
        
        return result
    except Exception as e:
        logger.error(f"Error searching components: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def search_by_property(nifi_client: NiFiAPIClient, property_name: str, property_value: Optional[str] = None,
                           pg_id: str = "root", recursive: bool = True) -> Dict[str, Any]:
    """Search for processors by property name and optionally value.
    
    Args:
        nifi_client: NiFi API client instance
        property_name: Name of the property to search for
        property_value: Optional value of the property to match
        pg_id: ID of the parent process group (default: root)
        recursive: Whether to search recursively in child process groups
        
    Returns:
        Dictionary with matching processors
    """
    try:
        matches = []
        
        # Get processors in the current process group
        response = nifi_client.get(f"/flow/process-groups/{pg_id}")
        processors = response.get("processGroupFlow", {}).get("flow", {}).get("processors", [])
        
        # For each processor, get detailed information to check properties
        for processor in processors:
            processor_id = processor.get("id")
            processor_detail = nifi_client.get(f"/processors/{processor_id}")
            properties = processor_detail.get("component", {}).get("properties", {})
            
            # Check if the property exists
            if property_name in properties:
                # If property_value is specified, check if it matches
                if property_value is None or property_value == properties[property_name]:
                    matches.append({
                        "id": processor_id,
                        "name": processor.get("name"),
                        "type": processor.get("component", {}).get("type"),
                        "pg_id": pg_id,
                        "property_value": properties.get(property_name),
                        "state": processor.get("status", {}).get("runStatus")
                    })
        
        # If recursive, also search in child process groups
        if recursive:
            child_groups = response.get("processGroupFlow", {}).get("flow", {}).get("processGroups", [])
            
            for child_group in child_groups:
                child_id = child_group.get("id")
                child_results = await search_by_property(
                    nifi_client, 
                    property_name, 
                    property_value, 
                    child_id, 
                    recursive
                )
                
                if child_results.get("status") == "success":
                    matches.extend(child_results.get("processors", []))
        
        return {
            "status": "success",
            "property_name": property_name,
            "property_value": property_value,
            "processors": matches,
            "count": len(matches)
        }
    except Exception as e:
        logger.error(f"Error searching by property: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def find_component_by_name(nifi_client: NiFiAPIClient, name: str, component_type: str = None,
                               pg_id: str = "root", recursive: bool = True) -> Dict[str, Any]:
    """Find a component by exact name (or close match).
    
    Args:
        nifi_client: NiFi API client instance
        name: Name to search for
        component_type: Type of component to search for (processor, process_group, etc.)
        pg_id: ID of the parent process group (default: root)
        recursive: Whether to search recursively in child process groups
        
    Returns:
        Dictionary with matching component information
    """
    try:
        name_lower = name.lower()
        matches = []
        
        # Get process group contents
        response = nifi_client.get(f"/flow/process-groups/{pg_id}")
        flow = response.get("processGroupFlow", {}).get("flow", {})
        
        # Check component type and search accordingly
        if component_type is None or component_type == "processor":
            processors = flow.get("processors", [])
            for item in processors:
                item_name = item.get("name", "").lower()
                if item_name == name_lower or name_lower in item_name:
                    matches.append({
                        "type": "processor",
                        "id": item.get("id"),
                        "name": item.get("name"),
                        "pg_id": pg_id,
                        "exact_match": item_name == name_lower
                    })
        
        if component_type is None or component_type == "process_group":
            process_groups = flow.get("processGroups", [])
            for item in process_groups:
                item_name = item.get("name", "").lower()
                if item_name == name_lower or name_lower in item_name:
                    matches.append({
                        "type": "process_group",
                        "id": item.get("id"),
                        "name": item.get("name"),
                        "pg_id": pg_id,
                        "exact_match": item_name == name_lower
                    })
        
        if component_type is None or component_type == "connection":
            connections = flow.get("connections", [])
            for item in connections:
                item_name = item.get("name", "").lower()
                if item_name == name_lower or name_lower in item_name:
                    matches.append({
                        "type": "connection",
                        "id": item.get("id"),
                        "name": item.get("name"),
                        "pg_id": pg_id,
                        "exact_match": item_name == name_lower
                    })
        
        # Search ports if applicable
        if component_type is None or component_type == "input_port":
            input_ports = flow.get("inputPorts", [])
            for item in input_ports:
                item_name = item.get("name", "").lower()
                if item_name == name_lower or name_lower in item_name:
                    matches.append({
                        "type": "input_port",
                        "id": item.get("id"),
                        "name": item.get("name"),
                        "pg_id": pg_id,
                        "exact_match": item_name == name_lower
                    })
        
        if component_type is None or component_type == "output_port":
            output_ports = flow.get("outputPorts", [])
            for item in output_ports:
                item_name = item.get("name", "").lower()
                if item_name == name_lower or name_lower in item_name:
                    matches.append({
                        "type": "output_port",
                        "id": item.get("id"),
                        "name": item.get("name"),
                        "pg_id": pg_id,
                        "exact_match": item_name == name_lower
                    })
        
        # If recursive, also search in child process groups
        if recursive:
            process_groups = flow.get("processGroups", [])
            
            for child_group in process_groups:
                child_id = child_group.get("id")
                child_results = await find_component_by_name(
                    nifi_client, 
                    name, 
                    component_type, 
                    child_id, 
                    recursive
                )
                
                if child_results.get("status") == "success":
                    matches.extend(child_results.get("matches", []))
        
        # Sort matches - exact matches first, then by name
        matches.sort(key=lambda x: (not x.get("exact_match", False), x.get("name", "")))
        
        return {
            "status": "success",
            "name": name,
            "component_type": component_type,
            "matches": matches,
            "count": len(matches)
        }
    except Exception as e:
        logger.error(f"Error finding component by name: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

# Additional search operations can be added here
