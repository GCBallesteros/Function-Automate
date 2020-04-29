import logging
from functools import wraps

import azure.functions as func


class HttpError(Exception):
    def __init__(self, message: str, response: func.HttpResponse):
        super().__init__(message)
        self.response = response


def exceptions_as_response(f):
    @wraps(f)
    def main_with_responses(req: func.HttpRequest) -> func.HttpResponse:
        try:
            return f(req)
        except HttpError as e:
            return e.response
        except Exception as e:
            logging.info(e.message)
            raise e

    return main_with_responses
