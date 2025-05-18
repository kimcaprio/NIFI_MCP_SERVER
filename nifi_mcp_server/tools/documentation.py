from typing import Dict, Any, List, Optional
from ..nifi_api import NiFiAPIClient
from loguru import logger

# Documentation Tools

async def get_processor_docs(nifi_client: NiFiAPIClient, processor_type: str) -> Dict[str, Any]:
    """Get documentation for a specific processor type.
    
    Args:
        nifi_client: NiFi API client instance
        processor_type: Type of processor (e.g., org.apache.nifi.processors.standard.GetFile)
        
    Returns:
        Dictionary with processor documentation
    """
    try:
        response = nifi_client.get(f"/flow/processor-types/{processor_type}")
        processor_types = response.get("processorTypes", [])
        
        if not processor_types:
            return {
                "status": "error",
                "message": f"Processor type not found: {processor_type}"
            }
        
        processor_info = processor_types[0]
        
        # Extract documentation
        docs = {
            "description": processor_info.get("description", ""),
            "tags": processor_info.get("tags", []),
            "input_requirement": processor_info.get("inputRequirement", ""),
            "properties": [
                {
                    "name": prop.get("name"),
                    "display_name": prop.get("displayName"),
                    "description": prop.get("description"),
                    "default_value": prop.get("defaultValue"),
                    "required": prop.get("required", False),
                    "sensitive": prop.get("sensitive", False),
                    "dynamic": prop.get("dynamic", False),
                    "supported_values": prop.get("allowableValues", {}).get("allowableValues", []),
                    "expression_language_scope": prop.get("expressionLanguageScope")
                }
                for prop in processor_info.get("propertyDescriptors", {}).values()
            ],
            "relationships": [
                {
                    "name": rel.get("name"),
                    "description": rel.get("description"),
                    "auto_terminate_allowed": rel.get("autoTerminateAllowed", False)
                }
                for rel in processor_info.get("relationshipDefinitions", [])
            ],
            "dynamic_properties_allowed": processor_info.get("supportsDynamicProperties", False),
            "stateful": processor_info.get("stateful", False),
            "restricted": processor_info.get("restricted", False),
            "bundle": processor_info.get("bundle", {})
        }
        
        return {
            "status": "success",
            "processor_type": processor_type,
            "documentation": docs
        }
    except Exception as e:
        logger.error(f"Error getting processor documentation: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def list_processor_types(nifi_client: NiFiAPIClient, tag: Optional[str] = None) -> Dict[str, Any]:
    """List available processor types, optionally filtered by tag.
    
    Args:
        nifi_client: NiFi API client instance
        tag: Optional tag to filter processors by
        
    Returns:
        Dictionary with processor type information
    """
    try:
        response = nifi_client.get("/flow/processor-types")
        processor_types = response.get("processorTypes", [])
        
        # Filter by tag if specified
        if tag:
            tag_lower = tag.lower()
            processor_types = [
                pt for pt in processor_types
                if any(t.lower() == tag_lower for t in pt.get("tags", []))
            ]
        
        # Extract relevant information
        result = [
            {
                "type": pt.get("type"),
                "bundle": pt.get("bundle", {}),
                "display_name": pt.get("typeDescription", ""),
                "tags": pt.get("tags", []),
                "restricted": pt.get("restricted", False)
            }
            for pt in processor_types
        ]
        
        # Sort by display name
        result.sort(key=lambda x: x.get("display_name", ""))
        
        return {
            "status": "success",
            "tag_filter": tag,
            "processor_types": result,
            "count": len(result)
        }
    except Exception as e:
        logger.error(f"Error listing processor types: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def get_controller_service_docs(nifi_client: NiFiAPIClient, service_type: str) -> Dict[str, Any]:
    """Get documentation for a specific controller service type.
    
    Args:
        nifi_client: NiFi API client instance
        service_type: Type of controller service
        
    Returns:
        Dictionary with controller service documentation
    """
    try:
        response = nifi_client.get(f"/flow/controller-service-types/{service_type}")
        service_types = response.get("controllerServiceTypes", [])
        
        if not service_types:
            return {
                "status": "error",
                "message": f"Controller service type not found: {service_type}"
            }
        
        service_info = service_types[0]
        
        # Extract documentation
        docs = {
            "description": service_info.get("description", ""),
            "tags": service_info.get("tags", []),
            "properties": [
                {
                    "name": prop.get("name"),
                    "display_name": prop.get("displayName"),
                    "description": prop.get("description"),
                    "default_value": prop.get("defaultValue"),
                    "required": prop.get("required", False),
                    "sensitive": prop.get("sensitive", False),
                    "dynamic": prop.get("dynamic", False),
                    "supported_values": prop.get("allowableValues", {}).get("allowableValues", []),
                    "expression_language_scope": prop.get("expressionLanguageScope")
                }
                for prop in service_info.get("propertyDescriptors", {}).values()
            ],
            "provided_api": service_info.get("providedApiImplementations", []),
            "dynamic_properties_allowed": service_info.get("supportsDynamicProperties", False),
            "restricted": service_info.get("restricted", False),
            "bundle": service_info.get("bundle", {})
        }
        
        return {
            "status": "success",
            "service_type": service_type,
            "documentation": docs
        }
    except Exception as e:
        logger.error(f"Error getting controller service documentation: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def list_controller_service_types(nifi_client: NiFiAPIClient, tag: Optional[str] = None) -> Dict[str, Any]:
    """List available controller service types, optionally filtered by tag.
    
    Args:
        nifi_client: NiFi API client instance
        tag: Optional tag to filter services by
        
    Returns:
        Dictionary with controller service type information
    """
    try:
        response = nifi_client.get("/flow/controller-service-types")
        service_types = response.get("controllerServiceTypes", [])
        
        # Filter by tag if specified
        if tag:
            tag_lower = tag.lower()
            service_types = [
                st for st in service_types
                if any(t.lower() == tag_lower for t in st.get("tags", []))
            ]
        
        # Extract relevant information
        result = [
            {
                "type": st.get("type"),
                "bundle": st.get("bundle", {}),
                "display_name": st.get("typeDescription", ""),
                "tags": st.get("tags", []),
                "restricted": st.get("restricted", False)
            }
            for st in service_types
        ]
        
        # Sort by display name
        result.sort(key=lambda x: x.get("display_name", ""))
        
        return {
            "status": "success",
            "tag_filter": tag,
            "controller_service_types": result,
            "count": len(result)
        }
    except Exception as e:
        logger.error(f"Error listing controller service types: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def get_reporting_task_docs(nifi_client: NiFiAPIClient, task_type: str) -> Dict[str, Any]:
    """Get documentation for a specific reporting task type.
    
    Args:
        nifi_client: NiFi API client instance
        task_type: Type of reporting task
        
    Returns:
        Dictionary with reporting task documentation
    """
    try:
        response = nifi_client.get(f"/flow/reporting-task-types/{task_type}")
        task_types = response.get("reportingTaskTypes", [])
        
        if not task_types:
            return {
                "status": "error",
                "message": f"Reporting task type not found: {task_type}"
            }
        
        task_info = task_types[0]
        
        # Extract documentation
        docs = {
            "description": task_info.get("description", ""),
            "tags": task_info.get("tags", []),
            "properties": [
                {
                    "name": prop.get("name"),
                    "display_name": prop.get("displayName"),
                    "description": prop.get("description"),
                    "default_value": prop.get("defaultValue"),
                    "required": prop.get("required", False),
                    "sensitive": prop.get("sensitive", False),
                    "dynamic": prop.get("dynamic", False),
                    "supported_values": prop.get("allowableValues", {}).get("allowableValues", []),
                    "expression_language_scope": prop.get("expressionLanguageScope")
                }
                for prop in task_info.get("propertyDescriptors", {}).values()
            ],
            "dynamic_properties_allowed": task_info.get("supportsDynamicProperties", False),
            "restricted": task_info.get("restricted", False),
            "bundle": task_info.get("bundle", {})
        }
        
        return {
            "status": "success",
            "task_type": task_type,
            "documentation": docs
        }
    except Exception as e:
        logger.error(f"Error getting reporting task documentation: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def list_expression_language_functions(nifi_client: NiFiAPIClient) -> Dict[str, Any]:
    """List available Expression Language functions.
    
    Args:
        nifi_client: NiFi API client instance
        
    Returns:
        Dictionary with Expression Language function information
    """
    try:
        # Expression Language functions data is not available through the API
        # We provide a static list of common functions
        
        el_functions = [
            {
                "name": "equals",
                "description": "Checks if one value equals another",
                "example": "${filename:equals('test.txt')}",
                "category": "Boolean Logic"
            },
            {
                "name": "toUpper",
                "description": "Converts the subject value to uppercase",
                "example": "${filename:toUpper()}",
                "category": "String Manipulation"
            },
            {
                "name": "toLower",
                "description": "Converts the subject value to lowercase",
                "example": "${filename:toLower()}",
                "category": "String Manipulation"
            },
            {
                "name": "substring",
                "description": "Returns a substring from the subject",
                "example": "${filename:substring(0, 5)}",
                "category": "String Manipulation"
            },
            {
                "name": "contains",
                "description": "Checks if the subject contains the given value",
                "example": "${filename:contains('txt')}",
                "category": "Searching"
            },
            {
                "name": "startsWith",
                "description": "Checks if the subject starts with the given value",
                "example": "${filename:startsWith('test')}",
                "category": "Searching"
            },
            {
                "name": "endsWith",
                "description": "Checks if the subject ends with the given value",
                "example": "${filename:endsWith('.txt')}",
                "category": "Searching"
            },
            {
                "name": "plus",
                "description": "Adds numbers or concatenates strings",
                "example": "${literal(1):plus(2)}",
                "category": "Mathematical Operations"
            },
            {
                "name": "minus",
                "description": "Subtracts one number from another",
                "example": "${literal(5):minus(2)}",
                "category": "Mathematical Operations"
            },
            {
                "name": "multiply",
                "description": "Multiplies two numbers",
                "example": "${literal(2):multiply(3)}",
                "category": "Mathematical Operations"
            },
            {
                "name": "divide",
                "description": "Divides one number by another",
                "example": "${literal(10):divide(2)}",
                "category": "Mathematical Operations"
            },
            {
                "name": "format",
                "description": "Formats dates",
                "example": "${now():format('yyyy-MM-dd')}",
                "category": "Date Manipulation"
            },
            {
                "name": "now",
                "description": "Returns the current date/time",
                "example": "${now()}",
                "category": "Date Manipulation"
            },
            {
                "name": "trim",
                "description": "Removes leading and trailing whitespace",
                "example": "${filename:trim()}",
                "category": "String Manipulation"
            },
            {
                "name": "replace",
                "description": "Replaces all occurrences of a string with another",
                "example": "${filename:replace('.txt', '.csv')}",
                "category": "String Manipulation"
            },
            {
                "name": "UUID",
                "description": "Generates a random UUID",
                "example": "${UUID()}",
                "category": "Subjectless Functions"
            }
        ]
        
        # Group by category
        categories = {}
        for func in el_functions:
            category = func["category"]
            if category not in categories:
                categories[category] = []
            categories[category].append(func)
        
        return {
            "status": "success",
            "categories": categories,
            "functions": el_functions,
            "count": len(el_functions)
        }
    except Exception as e:
        logger.error(f"Error listing Expression Language functions: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

# Additional documentation operations can be added here
