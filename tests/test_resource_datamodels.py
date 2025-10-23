#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/10/22
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================
from src.oshconnect.resource_datamodels import ControlStreamResource


def test_control_stream_resource():
    res_str = {'id': '0228vl6tn15g', 'name': 'Puppy Pi Control', 'description': 'Puppy pi control', 'system@id': '029tjlvogsng', 'system@link': {'href': 'http://192.168.8.136:8080/sensorhub/api/systems/029tjlvogsng?f=json', 'uid': 'urn:puppypi:001', 'type': 'application/geo+json'}, 'inputName': 'puppypicontrol', 'validTime': ['2025-10-21T19:04:56.505817Z', 'now'], 'issueTime': ['2025-10-22T17:12:58.51182Z', '2025-10-22T17:12:58.51182Z'], 'controlledProperties': [{'definition': 'http://sensorml.com/ont/swe/property/triggercontrol', 'label': 'Forward', 'description': 'Moves the puppy pi forward when true'}], 'formats': ['application/json', 'application/swe+json', 'application/swe+csv', 'application/swe+xml', 'application/swe+binary']}
    # res_dict = json.loads(res_str)
    csr = ControlStreamResource.model_validate(res_str)

    assert isinstance(csr, ControlStreamResource)
