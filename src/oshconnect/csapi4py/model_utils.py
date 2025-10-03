from pydantic import BaseModel


def serialize_model_list(model_list: list[BaseModel]):
    """
    Serializes a list of pydantic models
    :param model_list:
    :return: a valid json string
    """
    return '[' + ','.join([model.model_dump_json(exclude_none=True, by_alias=True) for model in model_list]) + ']'
