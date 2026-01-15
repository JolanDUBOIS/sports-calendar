from flask import request, jsonify, Blueprint, render_template

from . import logger
from sports_calendar.core.selection import SelectionRegistry, SelectionSpec


bp = Blueprint("selections", __name__, url_prefix="/selections")

@bp.route("/", methods=["GET"])
def list_selections():
    """ List all selections. """
    try:
        selections = SelectionRegistry.get_all()
    except Exception:
        logger.exception("Error retrieving selections")
        return jsonify({"error": "Internal server error"}), 500
    payload = {
        "selections": [
            {"selection": sel.to_dict()} for sel in selections.values()
        ]
    }
    return render_template("selections.html", **payload), 200

@bp.route("/", methods=["POST"])
def create_selection():
    """ Create a new selection. """
    data = request.get_json(silent=True)
    name = data.get("name") if data else None
    if not name:
        return jsonify({"error": "Selection name is required"}), 400
    logger.info(f"Creating selection with name: {name}")
    try:
        new_selection = SelectionSpec.empty(name=name)
        SelectionRegistry.add(new_selection)
        return jsonify({"selection": new_selection.to_dict()}), 201
    except Exception:
        logger.exception("Error creating selection")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/validate-name", methods=["POST"])
def validate_selection_name():
    """ Validate if a selection name is valid (i.e., not duplicate, not empty). """
    data = request.get_json(silent=True)
    name = data.get("name") if data else None
    if not name:
        return jsonify({"valid": False, "error": "Name cannot be empty."}), 200
    exists = SelectionRegistry.exists(name)
    if exists:
        return jsonify({"valid": False, "error": "Name already exists."}), 200
    return jsonify({"valid": True}), 200

@bp.route("/<sid>", methods=["GET"])
def get_selection(sid: str):
    """ Get a specific selection by id. """
    try:
        selection = SelectionRegistry.get(sid)
        payload = {"selection": selection.to_dict()}
        return render_template("selection/view.html", **payload), 200
    except KeyError:
        logger.warning(f"Selection not found: {sid}")
        return jsonify({"error": "Selection not found"}), 404
    except Exception:
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
        SelectionRegistry.remove(sid)
        return "", 204
    except KeyError:
        logger.warning(f"Selection not found for deletion: {sid}")
        return jsonify({"error": "Selection not found"}), 404
    except Exception:
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
        selection = SelectionRegistry.get(sid)
        cloned_selection = selection.clone(new_name=new_name)
        SelectionRegistry.add(cloned_selection)
        return jsonify({"selection": cloned_selection.to_dict()}), 201
    except KeyError:
        logger.warning(f"Selection not found for cloning: {sid}")
        return jsonify({"error": "Selection not found"}), 404
    except Exception:
        logger.exception(f"Error cloning selection: {sid}")
        return jsonify({"error": "Internal server error"}), 500
