import os
from typing import Dict, Any, List, Optional, Union
from pydantic import BaseModel
import openai
from loguru import logger

class QueryContext(BaseModel):
    """Context information for a natural language query."""
    query: str
    nifi_url: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    previous_queries: List[Dict[str, Any]] = []

class QueryIntent(BaseModel):
    """Detected intent from a natural language query."""
    intent_type: str
    confidence: float = 0.0
    parameters: Dict[str, Any] = {}
    
class QueryResult(BaseModel):
    """Result of processing a natural language query."""
    original_query: str
    detected_intent: Optional[QueryIntent] = None
    response: str
    action_taken: Optional[str] = None
    context_updates: Dict[str, Any] = {}
    error: Optional[str] = None

class NLProcessor:
    """Processor for natural language queries to Apache NiFi."""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-3.5-turbo"):
        """Initialize the NLP processor.
        
        Args:
            api_key: OpenAI API key (defaults to OPENAI_API_KEY environment variable)
            model: OpenAI model to use
        """
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self.api_key:
            logger.warning("No OpenAI API key provided. NLP processing will be limited.")
        
        self.model = model
        
        # Intent mappings for common NiFi operations
        self.intent_mappings = {
            "list_process_groups": ["list process groups", "show process groups", "get process groups"],
            "get_processor_details": ["processor details", "show processor", "get processor info"],
            "create_process_group": ["create process group", "new process group", "add process group"],
            "start_component": ["start processor", "start component", "run processor", "run flow", "start flow"],
            "stop_component": ["stop processor", "stop component", "halt processor", "stop flow", "pause flow"],
            "get_flow_status": ["flow status", "get status", "show status", "check status", "monitor flow"],
            "search_components": ["search for", "find components", "look for", "locate"],
            "list_processors": ["list processors", "show processors", "get processors"],
            "list_connections": ["list connections", "show connections", "get connections"],
            "list_templates": ["list templates", "show templates", "get templates"],
        }
    
    async def process_query(self, context: QueryContext) -> QueryResult:
        """Process a natural language query.
        
        Args:
            context: Query context including the query and other metadata
            
        Returns:
            QueryResult with detected intent and response
        """
        query = context.query.strip()
        
        try:
            # Basic intent detection without using OpenAI
            intent = self._detect_intent_simple(query)
            
            # If we have an API key, use OpenAI for more advanced processing
            if self.api_key and intent.confidence < 0.8:
                intent = await self._detect_intent_openai(query)
            
            # Generate a response based on the detected intent
            response = self._generate_response(query, intent)
            
            return QueryResult(
                original_query=query,
                detected_intent=intent,
                response=response,
                context_updates={}
            )
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            return QueryResult(
                original_query=query,
                response="I encountered an error processing your query.",
                error=str(e)
            )
    
    def _detect_intent_simple(self, query: str) -> QueryIntent:
        """Simple keyword-based intent detection.
        
        Args:
            query: The natural language query
            
        Returns:
            QueryIntent with the detected intent type and parameters
        """
        query_lower = query.lower()
        best_intent = None
        best_confidence = 0.0
        
        # Check each intent mapping for matches
        for intent_type, phrases in self.intent_mappings.items():
            for phrase in phrases:
                if phrase in query_lower:
                    confidence = len(phrase) / len(query_lower)
                    if confidence > best_confidence:
                        best_confidence = confidence
                        best_intent = intent_type
        
        # If no intent matched, default to search
        if not best_intent:
            return QueryIntent(intent_type="unknown", confidence=0.1)
        
        # Extract basic parameters based on the intent type
        parameters = self._extract_parameters(query, best_intent)
        
        return QueryIntent(
            intent_type=best_intent,
            confidence=best_confidence,
            parameters=parameters
        )
    
    async def _detect_intent_openai(self, query: str) -> QueryIntent:
        """Use OpenAI to detect the intent of a query.
        
        Args:
            query: The natural language query
            
        Returns:
            QueryIntent with the detected intent type and parameters
        """
        if not self.api_key:
            return self._detect_intent_simple(query)
        
        try:
            openai.api_key = self.api_key
            
            system_message = """
            You are an assistant that helps users interact with Apache NiFi through natural language.
            Your job is to classify the user's query into one of these intents:
            - list_process_groups: List process groups
            - get_processor_details: Get details of a specific processor
            - create_process_group: Create a new process group
            - start_component: Start a component
            - stop_component: Stop a component
            - get_flow_status: Get the status of the flow
            - search_components: Search for components
            - unknown: If the query doesn't match any of the above
            
            Also extract any relevant parameters from the query, like names, IDs, or other qualifiers.
            Return your response in JSON format with 'intent_type' and 'parameters' fields.
            """
            
            messages = [
                {"role": "system", "content": system_message},
                {"role": "user", "content": query}
            ]
            
            response = await openai.chat.completions.create(
                model=self.model,
                messages=messages,
                response_format={"type": "json_object"}
            )
            
            result = response.choices[0].message.content
            
            # Parse the JSON response
            import json
            parsed = json.loads(result)
            
            return QueryIntent(
                intent_type=parsed.get("intent_type", "unknown"),
                confidence=0.9,  # High confidence for OpenAI results
                parameters=parsed.get("parameters", {})
            )
        except Exception as e:
            logger.error(f"Error with OpenAI intent detection: {str(e)}")
            # Fall back to simple intent detection
            return self._detect_intent_simple(query)
    
    def _extract_parameters(self, query: str, intent_type: str) -> Dict[str, Any]:
        """Extract parameters from a query based on the intent type.
        
        Args:
            query: The natural language query
            intent_type: The detected intent type
            
        Returns:
            Dictionary of extracted parameters
        """
        # Basic parameter extraction
        params = {}
        query_lower = query.lower()
        
        # Extract parameters based on intent type
        if intent_type == "list_process_groups":
            # Look for "in [group name]" pattern
            if "in " in query_lower:
                group_name = query[query_lower.find("in ") + 3:].strip()
                params["parent_group"] = group_name
        
        elif intent_type in ["get_processor_details", "start_component", "stop_component"]:
            # Look for "processor [name]" or "[name] processor" patterns
            if "processor " in query_lower:
                processor_name = query[query_lower.find("processor ") + 10:].strip()
                params["name"] = processor_name
            elif " processor" in query_lower:
                processor_name = query[:query_lower.find(" processor")].strip()
                params["name"] = processor_name
        
        elif intent_type == "create_process_group":
            # Look for "named [name]" or "called [name]" patterns
            if "named " in query_lower:
                group_name = query[query_lower.find("named ") + 6:].strip()
                params["name"] = group_name
            elif "called " in query_lower:
                group_name = query[query_lower.find("called ") + 7:].strip()
                params["name"] = group_name
        
        elif intent_type == "search_components":
            # Extract search terms
            search_terms = []
            for pattern in ["search for ", "find ", "look for "]:
                if pattern in query_lower:
                    term = query[query_lower.find(pattern) + len(pattern):].strip()
                    search_terms.append(term)
                    break
            
            if search_terms:
                params["search_term"] = search_terms[0]
        
        return params
    
    def _generate_response(self, query: str, intent: QueryIntent) -> str:
        """Generate a response based on the detected intent.
        
        Args:
            query: The original query
            intent: The detected intent
            
        Returns:
            A natural language response
        """
        intent_type = intent.intent_type
        parameters = intent.parameters
        
        # Generate different responses based on the intent type
        if intent_type == "unknown":
            return "I'm not sure what you're asking about. You can ask about process groups, processors, or other NiFi components."
        
        elif intent_type == "list_process_groups":
            parent_group = parameters.get("parent_group", "root")
            return f"I'll list the process groups in {parent_group}."
        
        elif intent_type == "get_processor_details":
            processor_name = parameters.get("name", "the processor")
            return f"I'll get the details for {processor_name}."
        
        elif intent_type == "create_process_group":
            group_name = parameters.get("name", "the new process group")
            return f"I'll create a new process group called '{group_name}'."
        
        elif intent_type == "start_component":
            component_name = parameters.get("name", "the component")
            return f"I'll start {component_name}."
        
        elif intent_type == "stop_component":
            component_name = parameters.get("name", "the component")
            return f"I'll stop {component_name}."
        
        elif intent_type == "get_flow_status":
            return "I'll get the current status of your flow."
        
        elif intent_type == "search_components":
            search_term = parameters.get("search_term", "components")
            return f"I'll search for '{search_term}' in your NiFi instance."
        
        else:
            return f"I'll help you with: {intent_type.replace('_', ' ')}." 