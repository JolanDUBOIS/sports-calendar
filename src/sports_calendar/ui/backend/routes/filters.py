from flask import request, jsonify, Blueprint

from . import logger
from sports_calendar.core.selection import SelectionRegistry, SelectionFilterSpec


bp = Blueprint("filters", __name__, url_prefix="/selections/<sid>/items/<iid>/filters")

@bp.route("/", methods=["GET"])
def list_filters(sid: str, iid: str):
    """ List all filters in an item. """
    try:
        selection = SelectionRegistry.get(sid)
        item = selection.get_item(iid)
        return jsonify({"filters": [filt.to_dict() for filt in item.filters]}), 200
    except KeyError:
        logger.warning(f"Item not found: {iid} in selection {sid}")
        return jsonify({"error": "Item not found"}), 404
    except Exception:
        logger.exception(f"Error retrieving filters for item: {iid} in selection {sid}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/", methods=["POST"])
def create_filter(sid: str, iid: str):
    """ Create a new filter in an item. """
    data = request.get_json(silent=True)
    filter_type = data.get("filter_type") if data else None
    if not filter_type:
        return jsonify({"error": "Filter type is required"}), 400
    try:
        selection = SelectionRegistry.get(sid)
        item = selection.get_item(iid)
        new_filter = SelectionFilterSpec.empty(sport=item.sport, filter_type=filter_type)
        item.add_filter(new_filter)
        return jsonify({"filter": new_filter.to_dict()}), 201
    except KeyError:
        logger.warning(f"Item not found: {iid} in selection {sid}")
        return jsonify({"error": "Item not found"}), 404
    except Exception:
        logger.exception(f"Error creating filter in item: {iid} in selection {sid}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/<filter_id>", methods=["GET"])
def get_filter(sid: str, iid: str, filter_id: str):
    """ Get a specific filter by id in an item. """
    try:
        selection = SelectionRegistry.get(sid)
        item = selection.get_item(iid)
        filt = item.get_filter(filter_id)
        return jsonify({"filter": filt.to_dict()}), 200
    except KeyError:
        logger.warning(f"Filter not found: {filter_id} in item {iid} of selection {sid}")
        return jsonify({"error": "Filter not found"}), 404
    except Exception:
        logger.exception(f"Error retrieving filter: {filter_id} in item {iid} of selection {sid}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/<filter_id>", methods=["PUT"])
def update_filter(sid: str, iid: str, filter_id: str):
    """ Update a specific filter by id in an item. """
    return jsonify({"message": "Not implemented"}), 501

@bp.route("/<filter_id>", methods=["DELETE"])
def delete_filter(sid: str, iid: str, filter_id: str):
    """ Delete a specific filter by id in an item. """
    try:
        selection = SelectionRegistry.get(sid)
        item = selection.get_item(iid)
        item.remove_filter(filter_id)
        return "", 204
    except KeyError:
        logger.warning(f"Filter not found for deletion: {filter_id} in item {iid} of selection {sid}")
        return jsonify({"error": "Filter not found"}), 404
    except Exception:
        logger.exception(f"Error deleting filter: {filter_id} in item {iid} of selection {sid}")
        return jsonify({"error": "Internal server error"}), 500
