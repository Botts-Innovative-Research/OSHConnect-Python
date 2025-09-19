from enum import Enum


class APITerms(Enum):
    """
    Defines common endpoint terms used in the API
    """
    API = 'api'
    COLLECTIONS = 'collections'
    COMMANDS = 'commands'
    COMPONENTS = 'components'
    CONFORMANCE = 'conformance'
    CONTROL_STREAMS = 'controlstreams'
    DATASTREAMS = 'datastreams'
    DEPLOYMENTS = 'deployments'
    EVENTS = 'events'
    FOIS = 'featuresOfInterest'
    HISTORY = 'history'
    ITEMS = 'items'
    OBSERVATIONS = 'observations'
    PROCEDURES = 'procedures'
    PROPERTIES = 'properties'
    SAMPLING_FEATURES = 'samplingFeatures'
    SCHEMA = 'schema'
    STATUS = 'status'
    SYSTEMS = 'systems'
    SYSTEM_EVENTS = 'systemEvents'
    TASKING = 'controls'
    UNDEFINED = ''


class SystemTypes(Enum):
    """
    Defines the system types
    """
    FEATURE = "Feature"


class ObservationFormat(Enum):
    """
    Defines common observation formats
    """
    JSON = "application/om+json"
    XML = "application/om+xml"
    SWE_XML = "application/swe+xml"
    SWE_JSON = "application/swe+json"
    SWE_CSV = "application/swe+csv"
    SWE_BINARY = "application/swe+binary"
    SWE_TEXT = "application/swe+text"


class DatastreamResultTypes(Enum):
    """
    Defines the datastream result types
    """
    MEASURE = "measure"
    VECTOR = "vector"
    RECORD = "record"
    COVERAGE_1D = "coverage1D"
    COVERAGE_2D = "coverage2D"
    COVERAGE_3D = "coverage3D"


class GeometryTypes(Enum):
    """
    Defines the geometry types
    """
    POINT = "Point"
    LINESTRING = "LineString"
    POLYGON = "Polygon"
    MULTI_POINT = "MultiPoint"
    MULTI_LINESTRING = "MultiLineString"
    MULTI_POLYGON = "MultiPolygon"


class APIResourceTypes(Enum):
    """
    Defines the resource types
    """
    COLLECTION = "Collection"
    COMMAND = "Command"
    COMPONENT = "Component"
    CONTROL_CHANNEL = "ControlChannel"
    DATASTREAM = "Datastream"
    DEPLOYMENT = "Deployment"
    OBSERVATION = "Observation"
    PROCEDURE = "Procedure"
    PROPERTY = "Property"
    SAMPLING_FEATURE = "SamplingFeature"
    SYSTEM = "System"
    SYSTEM_EVENT = "SystemEvent"
    SYSTEM_HISTORY = "SystemHistory"


class EncodingSchema(Enum):
    """
    Defines the encoding formats
    """
    JSON = "application/json"
    XML = "application/xml"
    SWE_XML = "application/swe+xml"
    SWE_JSON = "application/swe+json"
    SWE_CSV = "application/swe+csv"
    SWE_BINARY = "application/swe+binary"
    SWE_TEXT = "application/swe+text"
    GEO_JSON = "application/geo+json"
    SML_JSON = "application/sml+json"
    OM_JSON = "application/om+json"
