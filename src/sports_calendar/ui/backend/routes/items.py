from flask import request, jsonify, Blueprint

from . import logger
from ..presenters import SelectionItemPresenter
from sports_calendar.core.selection import SelectionRegistry, SelectionItemSpec


bp = Blueprint("items", __name__, url_prefix="/selections/<sid>/items")

@bp.route("/", methods=["GET"])
def list_items(sid: str):
    """ List all items in a selection. """
    try:
        selection = SelectionRegistry.get(sid)
        return jsonify({"items": [SelectionItemPresenter.summary(item) for item in selection.items]}), 200
    except KeyError:
        logger.warning(f"Selection not found: {sid}")
        return jsonify({"error": "Selection not found"}), 404
    except Exception:
        logger.exception(f"Error retrieving items for selection: {sid}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/", methods=["POST"])
def create_item(sid: str):
    """ Create a new item in a selection. """
    data = request.get_json(silent=True)
    sport = data.get("sport") if data else None
    if not sport:
        return jsonify({"error": "Item sport is required"}), 400
    try:
        selection = SelectionRegistry.get(sid)
        new_item = SelectionItemSpec.empty(sport=sport)
        selection.add_item(new_item)
        return jsonify({"item": SelectionItemPresenter.summary(new_item)}), 201
    except KeyError:
        logger.warning(f"Selection not found: {sid}")
        return jsonify({"error": "Selection not found"}), 404
    except Exception:
        logger.exception(f"Error creating item in selection: {sid}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/<iid>", methods=["GET"])
def get_item(sid: str, iid: str):
    """ Get a specific item by id in a selection. """
    try:
        selection = SelectionRegistry.get(sid)
        item = selection.get_item(iid)
        return jsonify({"item": SelectionItemPresenter.detailed(item)}), 200
    except KeyError:
        logger.warning(f"Item not found: {iid} in selection {sid}")
        return jsonify({"error": "Item not found"}), 404
    except Exception:
        logger.exception(f"Error retrieving item: {iid} in selection {sid}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/<iid>", methods=["PUT"])
def update_item(sid: str, iid: str):
    """ Update a specific item by id in a selection. """
    return jsonify({"message": "Not implemented"}), 501

@bp.route("/<iid>", methods=["DELETE"])
def delete_item(sid: str, iid: str):
    """ Delete a specific item by id in a selection. """
    try:
        selection = SelectionRegistry.get(sid)
        selection.remove_item(iid)
        return jsonify({"message": "Item deleted"}), 200
    except KeyError:
        logger.warning(f"Item not found for deletion: {iid} in selection {sid}")
        return jsonify({"error": "Item not found"}), 404
    except Exception:
        logger.exception(f"Error deleting item: {iid} in selection {sid}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/<iid>/clone", methods=["POST"])
def clone_item(sid: str, iid: str):
    """ Clone a specific item by id in a selection. """
    try:
        selection = SelectionRegistry.get(sid)
        item = selection.get_item(iid)
        cloned_item = item.clone()
        selection.add_item(cloned_item)
        return jsonify({"item": SelectionItemPresenter.summary(cloned_item)}), 201
    except KeyError:
        logger.warning(f"Item not found for cloning: {iid} in selection {sid}")
        return jsonify({"error": "Item not found"}), 404
    except Exception:
        logger.exception(f"Error cloning item: {iid} in selection {sid}")
        return jsonify({"error": "Internal server error"}), 500
