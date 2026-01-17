from flask import request, jsonify, Blueprint

from . import logger
from ..presenters import SelectionItemPresenter
from sports_calendar.core.selection import SelectionService


bp = Blueprint("items", __name__, url_prefix="/selections/<sname>/items")

@bp.route("/", methods=["GET"])
def list_items(sname: str):
    """ List all items in a selection. """
    try:
        selection = SelectionService.get_selection(sname)
        return jsonify({"items": [SelectionItemPresenter.summary(item) for item in selection.items]}), 200
    except KeyError:
        logger.warning(f"Selection not found: {sname}")
        return jsonify({"error": "Selection not found"}), 404
    except Exception:
        logger.exception(f"Error retrieving items for selection: {sname}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/", methods=["POST"])
def create_item(sname: str):
    """ Create a new item in a selection. """
    data = request.get_json(silent=True)
    sport = data.get("sport") if data else None
    if not sport:
        return jsonify({"error": "Item sport is required"}), 400
    try:
        new_item = SelectionService.add_empty_item(selection_name=sname, sport=sport)
        return jsonify({"item": SelectionItemPresenter.summary(new_item)}), 201
    except KeyError:
        logger.warning(f"Selection not found: {sname}")
        return jsonify({"error": "Selection not found"}), 404
    except Exception:
        logger.exception(f"Error creating item in selection: {sname}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/<iid>", methods=["GET"])
def get_item(sname: str, iid: str):
    """ Get a specific item by id in a selection. """
    try:
        item = SelectionService.get_item(selection_name=sname, item_uid=iid)
        return jsonify({"item": SelectionItemPresenter.detailed(item)}), 200
    except KeyError:
        logger.warning(f"Item not found: {iid} in selection {sname}")
        return jsonify({"error": "Item not found"}), 404
    except Exception:
        logger.exception(f"Error retrieving item: {iid} in selection {sname}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/<iid>", methods=["PUT"])
def update_item(sname: str, iid: str):
    """ Update a specific item by id in a selection. """
    return jsonify({"message": "Not implemented"}), 501

@bp.route("/<iid>", methods=["DELETE"])
def delete_item(sname: str, iid: str):
    """ Delete a specific item by id in a selection. """
    try:
        SelectionService.remove_item(selection_name=sname, item_uid=iid)
        return jsonify({"message": "Item deleted"}), 200
    except KeyError:
        logger.warning(f"Item not found for deletion: {iid} in selection {sname}")
        return jsonify({"error": "Item not found"}), 404
    except Exception:
        logger.exception(f"Error deleting item: {iid} in selection {sname}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/<iid>/clone", methods=["POST"])
def clone_item(sname: str, iid: str):
    """ Clone a specific item by id in a selection. """
    try:
        cloned_item = SelectionService.clone_item(selection_name=sname, item_uid=iid)
        return jsonify({"item": SelectionItemPresenter.summary(cloned_item)}), 201
    except KeyError:
        logger.warning(f"Item not found for cloning: {iid} in selection {sname}")
        return jsonify({"error": "Item not found"}), 404
    except Exception:
        logger.exception(f"Error cloning item: {iid} in selection {sname}")
        return jsonify({"error": "Internal server error"}), 500
