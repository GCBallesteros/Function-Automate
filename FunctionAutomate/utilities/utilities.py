"""Common utility functions.

Author: Guillem Ballesteros
"""
from typing import Any

import azure.functions as func


def get_param(request: func.HttpRequest, param_name: str) -> Any:
    """Extract parameter from incoming request.

    Parameters
    ----------
    request
        Incoming request
    param_name
        Name of the parameter that it will attempt to extract.
    """
    param = request.params.get(param_name)
    if not param:
        try:
            req_body = request.get_json()
        except ValueError:
            param = None
        else:
            param = req_body.get(param_name)

    if not param:
        return None
    else:
        return param
