from flask import request, jsonify, Blueprint

from . import logger
from sports_calendar.core.selection import SelectionManager


bp = Blueprint("selections", __name__, url_prefix="/selections")

@bp.route("/", methods=["GET"])
def list_selections():
    """ List all selections. """
    try:
        selections = SelectionManager.get_all()
    except Exception as e:
        logger.exception("Error retrieving selections")
        return jsonify({"error": "Internal server error"}), 500
    payload = {
        "selections": [
            {"name": sel.name, "sid": sel.uid} for sel in selections.values()
        ]
    }
    return jsonify(payload), 200

@bp.route("/", methods=["POST"])
def create_selection():
    """ Create a new selection. """
    data = request.get_json(silent=True)
    name = data.get("name") if data else None
    if not name:
        return jsonify({"error": "Selection name is required"}), 400

    try:
        new_selection = SelectionManager.empty(name=name)
        return jsonify({"name": new_selection.name, "sid": new_selection.uid}), 201
    except Exception as e:
        logger.exception("Error creating selection")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/<sid>", methods=["GET"])
def get_selection(sid: str):
    """ Get a specific selection by id. """
    try:
        selection = SelectionManager.get(sid)
        return jsonify(selection.to_dict()), 200
    except KeyError as e:
        logger.warning(f"Selection not found: {sid}")
        return jsonify({"error": "Selection not found"}), 404
    except Exception as e:
        logger.exception(f"Error retrieving selection: {sid}")
        return jsonify({"error": "Internal server error"}), 500  

@bp.route("/<sid>", methods=["PUT"])
def update_selection(sid: str):
    """ Update a specific selection by id. """
    return jsonify({"message": "Not implemented"}), 501

@bp.route("/<sid>", methods=["DELETE"])
def delete_selection(sid: str):
    """ Delete a specific selection by id. """
    try:
        SelectionManager.remove(sid)
        return "", 204
    except KeyError as e:
        logger.warning(f"Selection not found for deletion: {sid}")
        return jsonify({"error": "Selection not found"}), 404
    except Exception as e:
        logger.exception(f"Error deleting selection: {sid}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/<sid>/clone", methods=["POST"])
def clone_selection(sid: str):
    """ Clone a specific selection by id. """
    data = request.get_json(silent=True)
    new_name = data.get("name") if data else None
    if not new_name:
        return jsonify({"error": "New selection name is required for cloning"}), 400
    
    try:
        cloned_selection = SelectionManager.clone(sid, new_name=new_name)
        return jsonify({"name": cloned_selection.name, "sid": cloned_selection.uid}), 201
    except KeyError as e:
        logger.warning(f"Selection not found for cloning: {sid}")
        return jsonify({"error": "Selection not found"}), 404
    except Exception as e:
        logger.exception(f"Error cloning selection: {sid}")
        return jsonify({"error": "Internal server error"}), 500
