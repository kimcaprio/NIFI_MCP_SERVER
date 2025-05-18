import pytest
from nifi_mcp_server.nlp_processor import NLProcessor, QueryContext, QueryIntent

# Test the NLProcessor class
def test_nlp_processor_initialization():
    """Test that the NLProcessor initializes correctly."""
    processor = NLProcessor(api_key=None, model="gpt-3.5-turbo")
    assert processor.model == "gpt-3.5-turbo"
    assert processor.api_key is None

# Test intent detection
def test_simple_intent_detection():
    """Test the simple intent detection logic."""
    processor = NLProcessor(api_key=None)
    
    # Test process group listing intent
    intent = processor._detect_intent_simple("list all process groups")
    assert intent.intent_type == "list_process_groups"
    assert intent.confidence > 0.0
    
    # Test processor details intent
    intent = processor._detect_intent_simple("show me processor details for GetFile")
    assert intent.intent_type == "get_processor_details"
    assert intent.confidence > 0.0
    assert intent.parameters.get("name") == "GetFile"
    
    # Test create process group intent
    intent = processor._detect_intent_simple("create a new process group called Data Processing")
    assert intent.intent_type == "create_process_group"
    assert intent.confidence > 0.0
    assert intent.parameters.get("name") == "Data Processing"
    
    # Test unknown intent
    intent = processor._detect_intent_simple("hello world")
    assert intent.intent_type == "unknown"
    assert intent.confidence == 0.1

# Test parameter extraction
def test_parameter_extraction():
    """Test the parameter extraction logic."""
    processor = NLProcessor(api_key=None)
    
    # Test process group parameters
    params = processor._extract_parameters("list process groups in Data Flow", "list_process_groups")
    assert params.get("parent_group") == "Data Flow"
    
    # Test processor parameters
    params = processor._extract_parameters("show details for processor GetFile", "get_processor_details")
    assert params.get("name") == "GetFile"
    
    # Test search parameters
    params = processor._extract_parameters("search for GetFile processors", "search_components")
    assert params.get("search_term") == "GetFile processors"

# Test response generation
def test_response_generation():
    """Test the response generation logic."""
    processor = NLProcessor(api_key=None)
    
    # Test unknown response
    intent = QueryIntent(intent_type="unknown", confidence=0.1)
    response = processor._generate_response("hello", intent)
    assert "not sure" in response.lower()
    
    # Test list process groups response
    intent = QueryIntent(
        intent_type="list_process_groups", 
        confidence=0.8,
        parameters={"parent_group": "Data Flow"}
    )
    response = processor._generate_response("list process groups", intent)
    assert "Data Flow" in response
    
    # Test create process group response
    intent = QueryIntent(
        intent_type="create_process_group", 
        confidence=0.8,
        parameters={"name": "Test Group"}
    )
    response = processor._generate_response("create a process group", intent)
    assert "Test Group" in response 