from flask import request, make_response
from .constants import DATES
from flask import request, make_response

from .constants import DATES


def check_payload(request_payload, options, category, sub_list):
    primary_error = 'Invalid value in request body.'
    default_error = make_response(primary_error, 400)

    if not isinstance(request_payload.json, dict):
        return default_error

    if options is not None:
        if not set(request_payload.json.keys()).issubset(options):
            return make_response("Invalid keys in options. Allowed options: " + " ".join(options), 400)

    if category is not None:
        if not isinstance(request.json[category], dict):
            return default_error

    if sub_list is not None:
        if not set(request.json[category].keys()).issubset([DATES, sub_list]):
            return make_response('Invalid value in search requests. Check papers.', 400)
    return None


