from pydantic import BaseModel, HttpUrl


class NetworkProperties(BaseModel):
    endpoint_url: HttpUrl
    tls: bool = False
    stream_protocol: str = 'ws'
    mqtt_opts: dict = None
    mqtt_endpoint_url: HttpUrl = None
    connector_opts: dict = None
