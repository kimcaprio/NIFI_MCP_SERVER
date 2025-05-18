from typing import Dict, Any, List, Optional
from ..nifi_api import NiFiAPIClient
from loguru import logger

# Templates Tools

async def list_templates(nifi_client: NiFiAPIClient) -> Dict[str, Any]:
    """List all available templates.
    
    Args:
        nifi_client: NiFi API client instance
        
    Returns:
        Dictionary with template information
    """
    try:
        response = nifi_client.get("/flow/templates")
        templates = response.get("templates", [])
        
        # Extract relevant information
        result = [
            {
                "id": template.get("id"),
                "name": template.get("name"),
                "description": template.get("description"),
                "timestamp": template.get("timestamp"),
                "uri": template.get("uri")
            }
            for template in templates
        ]
        
        return {
            "status": "success",
            "templates": result,
            "count": len(result)
        }
    except Exception as e:
        logger.error(f"Error listing templates: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def get_template_details(nifi_client: NiFiAPIClient, template_id: str) -> Dict[str, Any]:
    """Get detailed information about a specific template.
    
    Args:
        nifi_client: NiFi API client instance
        template_id: Template ID
        
    Returns:
        Dictionary with template details
    """
    try:
        response = nifi_client.get(f"/templates/{template_id}")
        template = response.get("template", {})
        
        # Get the snippet information
        snippet = template.get("snippet", {})
        
        # Extract component counts
        processors = snippet.get("processors", {})
        connections = snippet.get("connections", {})
        process_groups = snippet.get("processGroups", {})
        input_ports = snippet.get("inputPorts", {})
        output_ports = snippet.get("outputPorts", {})
        
        return {
            "status": "success",
            "id": template_id,
            "name": template.get("name"),
            "description": template.get("description"),
            "timestamp": template.get("timestamp"),
            "encoding_version": template.get("encodingVersion"),
            "component_counts": {
                "processors": len(processors),
                "connections": len(connections),
                "process_groups": len(process_groups),
                "input_ports": len(input_ports),
                "output_ports": len(output_ports),
            },
            "processor_types": list(set([p.get("type") for p in processors.values()]))
        }
    except Exception as e:
        logger.error(f"Error getting template details: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def instantiate_template(nifi_client: NiFiAPIClient, template_id: str, pg_id: str,
                             position_x: int = 0, position_y: int = 0) -> Dict[str, Any]:
    """Instantiate a template in a process group.
    
    Args:
        nifi_client: NiFi API client instance
        template_id: ID of the template to instantiate
        pg_id: ID of the process group to instantiate the template in
        position_x: X position (default: 0)
        position_y: Y position (default: 0)
        
    Returns:
        Dictionary with operation status
    """
    try:
        # Prepare the request body
        request_body = {
            "templateId": template_id,
            "originX": position_x,
            "originY": position_y
        }
        
        # Make the API call
        response = nifi_client.post(f"/process-groups/{pg_id}/template-instance", request_body)
        
        # Extract the created flow from the response
        flow = response.get("flow", {})
        
        return {
            "status": "success",
            "template_id": template_id,
            "parent_group_id": pg_id,
            "position": {
                "x": position_x,
                "y": position_y
            },
            "components": {
                "processors": flow.get("processors", []),
                "connections": flow.get("connections", []),
                "process_groups": flow.get("processGroups", []),
                "input_ports": flow.get("inputPorts", []),
                "output_ports": flow.get("outputPorts", []),
                "labels": flow.get("labels", []),
                "funnels": flow.get("funnels", []),
                "remote_process_groups": flow.get("remoteProcessGroups", [])
            }
        }
    except Exception as e:
        logger.error(f"Error instantiating template: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def create_template(nifi_client: NiFiAPIClient, pg_id: str, name: str, 
                        description: Optional[str] = None, snippet_id: str = None) -> Dict[str, Any]:
    """Create a template from a process group or snippet.
    
    Args:
        nifi_client: NiFi API client instance
        pg_id: ID of the process group to create the template from, or the parent of the snippet
        name: Name for the new template
        description: Optional description for the template
        snippet_id: Optional snippet ID to create the template from
        
    Returns:
        Dictionary with new template information
    """
    try:
        # If no snippet ID is provided, we need to create a snippet from the process group
        if not snippet_id:
            # Get the process group info
            pg_response = nifi_client.get(f"/process-groups/{pg_id}")
            
            # Create a snippet that includes the entire process group
            snippet_request = {
                "snippet": {
                    "processGroups": {pg_id: {"id": pg_id}},
                    "parentGroupId": pg_response.get("component", {}).get("parentGroupId")
                }
            }
            
            snippet_response = nifi_client.post("/snippets", snippet_request)
            snippet_id = snippet_response.get("snippet", {}).get("id")
        
        # Prepare the request body for creating the template
        request_body = {
            "name": name,
            "description": description,
            "snippetId": snippet_id
        }
        
        # Make the API call
        response = nifi_client.post(f"/process-groups/{pg_id}/templates", request_body)
        
        return {
            "status": "success",
            "id": response.get("id"),
            "name": response.get("name"),
            "description": response.get("description"),
            "uri": response.get("uri")
        }
    except Exception as e:
        logger.error(f"Error creating template: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def delete_template(nifi_client: NiFiAPIClient, template_id: str) -> Dict[str, Any]:
    """Delete a template.
    
    Args:
        nifi_client: NiFi API client instance
        template_id: ID of the template to delete
        
    Returns:
        Dictionary with deletion status
    """
    try:
        # Make the API call
        nifi_client.delete(f"/templates/{template_id}")
        
        return {
            "status": "success",
            "message": f"Template {template_id} deleted successfully"
        }
    except Exception as e:
        logger.error(f"Error deleting template: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def export_template(nifi_client: NiFiAPIClient, template_id: str) -> Dict[str, Any]:
    """Export a template as XML.
    
    Args:
        nifi_client: NiFi API client instance
        template_id: ID of the template to export
        
    Returns:
        Dictionary with template XML
    """
    try:
        # Make the API call - this returns XML directly
        xml_response = nifi_client._make_request(
            "GET", 
            f"/templates/{template_id}/download", 
            headers={"Accept": "application/xml"}
        )
        
        return {
            "status": "success",
            "template_id": template_id,
            "template_xml": xml_response  # This will be XML content as a string
        }
    except Exception as e:
        logger.error(f"Error exporting template: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

# Additional template operations can be added here
