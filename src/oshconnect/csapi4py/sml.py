from pydantic import BaseModel, HttpUrl, Field


class TypeOf(BaseModel):
    """
    TypeOf is a resolvable reference to some other general process (that can be any type inheriting from AbstractProcess)
    :param href: The URL of the referenced process
    :param relationship: The relationship of the referenced process to the current process
    :param media_type: The media type of the referenced process
    :param href_lang: The language of the referenced process
    :param title: The title of the referenced process
    :param uid: The unique identifier of the referenced process
    :param target_resource: The target resource of the referenced process
    :param interface: The interface of the referenced process
    """
    href: HttpUrl
    relationship: str = Field(..., serialization_alias='rel')
    media_type: str = Field(None, serialization_alias='type')
    href_lang: str = Field(None, serialization_alias='hreflang')
    title: str = Field(None)
    uid: str = Field(None)
    target_resource: str = Field(None, serialization_alias='rt')
    interface: str = Field(None, serialization_alias='if')


class SMLAbstractProcess(BaseModel):
    description: str = None
    unique_id: str = Field(None, serialization_alias='uniqueID')
    label: str = None
    lang: str = None
    keywords: list = None
    identifiers: list = None
    classifiers: list = None
    valid_time: list = Field(None, serialization_alias='validTime')
    security_constraints: list = Field(None, serialization_alias='securityConstraints')
    legal_constraints: list = Field(None, serialization_alias='legalConstraints')
    characteristics: list = None
    capabilities: list = None
    contacts: list = None
    documents: list = None
    history: list = None
    definition: HttpUrl = None
    type_of: TypeOf = Field(None, serialization_alias='typeOf')
    configuration: HttpUrl = None
    features_of_interest: list = Field(None, serialization_alias='featuresOfInterest')
    inputs: list = None
    outputs: list = None
    parameters: list = None
    modes: list = None
    method: str = None
    position: list = None
