from typing import Union

import requests
from pydantic import HttpUrl


def get_request(url: HttpUrl, params: dict = None, headers: dict = None, auth: tuple = None):
    """
    Sends a GET request to the provided URL with the given parameters and headers
    :param url:
    :param params:
    :param headers:
    :param auth:
    :return: the response of the request
    """
    return requests.get(url, params=params, headers=headers, auth=auth)


def post_request(url: HttpUrl, body: Union[str, dict] = None, headers: dict = None, auth: tuple = None):
    """
    Sends a POST request to the provided URL with the given content and headers
    :param url:
    :param content_json:
    :param headers:
    :param auth:
    :return: the response of the request
    """
    if isinstance(body, str):
        return requests.post(url, data=body, headers=headers, auth=auth)
    else:
        return requests.post(url, json=body, headers=headers, auth=auth)


def put_request(url: HttpUrl, body: Union[str, dict] = None, headers: dict = None, auth: tuple = None):
    """
    Sends a PUT request to the provided URL with the given content and headers
    :param url:
    :param content_json:
    :param headers:
    :param auth:
    :return: the response of the request
    """
    if isinstance(body, str):
        return requests.put(url, data=body, headers=headers, auth=auth)
    else:
        return requests.put(url, json=body, headers=headers, auth=auth)


def delete_request(url: HttpUrl, params: dict = None, headers: dict = None, auth: tuple = None):
    """
    Sends a DELETE request to the provided URL with the given parameters and headers
    :param url:
    :param params:
    :param headers:
    :param auth:
    :return: the response of the request
    """
    return requests.delete(url, params=params, headers=headers, auth=auth)
