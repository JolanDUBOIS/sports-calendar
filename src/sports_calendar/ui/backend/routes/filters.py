from flask import request, jsonify, Blueprint

from . import logger
from ..readers import FilterReader
from ..presenters import FilterPresenter
from sports_calendar.core.selection import SelectionService


bp = Blueprint("filters", __name__, url_prefix="/selections/<sname>/items/<iid>/filters")

@bp.route("/", methods=["GET"])
def list_filters(sname: str, iid: str):
    """ List all filters in an item. """
    try:
        item = SelectionService.get_item(selection_name=sname, item_uid=iid)
        return jsonify({"filters": [FilterPresenter.summary(filt) for filt in item.filters]}), 200
    except KeyError:
        logger.warning(f"Item not found: {iid} in selection {sname}")
        return jsonify({"error": "Item not found"}), 404
    except Exception:
        logger.exception(f"Error retrieving filters for item: {iid} in selection {sname}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/", methods=["POST"])
def create_filter(sname: str, iid: str):
    """ Create a new filter in an item. """
    data = request.get_json(silent=True)
    filter_type = data.get("filter_type") if data else None
    if not filter_type:
        return jsonify({"error": "Filter type is required"}), 400
    try:
        new_filter = SelectionService.add_empty_filter(selection_name=sname, item_uid=iid, filter_type=filter_type)
        return jsonify({"filter": FilterPresenter.summary(new_filter)}), 201
    except KeyError:
        logger.warning(f"Item not found: {iid} in selection {sname}")
        return jsonify({"error": "Item not found"}), 404
    except Exception:
        logger.exception(f"Error creating filter in item: {iid} in selection {sname}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/<fid>", methods=["GET"])
def get_filter(sname: str, iid: str, fid: str):
    """ Get a specific filter by id in an item. """
    try:
        filter = SelectionService.get_filter(selection_name=sname, item_uid=iid, filter_uid=fid)
        return jsonify({"filter": FilterPresenter.detailed(filter)}), 200
    except KeyError:
        logger.warning(f"Filter not found: {fid} in item {iid} of selection {sname}")
        return jsonify({"error": "Filter not found"}), 404
    except Exception:
        logger.exception(f"Error retrieving filter: {fid} in item {iid} of selection {sname}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/<fid>", methods=["PUT"])
def update_filter(sname: str, iid: str, fid: str):
    """ Update a specific filter by id in an item. """
    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"error": "Invalid JSON payload"}), 400
    
    logger.debug(f"Update filter called with data: {data}")
    try:
        original_filter = SelectionService.get_filter(selection_name=sname, item_uid=iid, filter_uid=fid)
        updated_filter = FilterReader.from_payload(data, original_filter)
        SelectionService.replace_filter(selection_name=sname, item_uid=iid, filter=updated_filter)
        return jsonify({"filter": FilterPresenter.detailed(updated_filter)}), 200
    except KeyError:
        logger.warning(f"Filter not found for update: {fid} in item {iid} of selection {sname}")
        return jsonify({"error": "Filter not found"}), 404
    except Exception:
        logger.exception(f"Error updating filter: {fid} in item {iid} of selection {sname}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/<fid>", methods=["DELETE"])
def delete_filter(sname: str, iid: str, fid: str):
    """ Delete a specific filter by id in an item. """
    try:
        SelectionService.remove_filter(selection_name=sname, item_uid=iid, filter_uid=fid)
        return "", 204
    except KeyError:
        logger.warning(f"Filter not found for deletion: {fid} in item {iid} of selection {sname}")
        return jsonify({"error": "Filter not found"}), 404
    except Exception:
        logger.exception(f"Error deleting filter: {fid} in item {iid} of selection {sname}")
        return jsonify({"error": "Internal server error"}), 500
