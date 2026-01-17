from flask import request, jsonify, Blueprint, render_template

from . import logger
from ..presenters import SelectionPresenter
from sports_calendar.core.selection import SelectionService


bp = Blueprint("selections", __name__, url_prefix="/selections")

@bp.route("/", methods=["GET"])
def list_selections():
    """ List all selections. """
    try:
        selections = SelectionService.get_all_selections()
    except Exception:
        logger.exception("Error retrieving selections")
        return jsonify({"error": "Internal server error"}), 500
    payload = {
        "selections": [
            {"selection": SelectionPresenter.summary(sel)} for sel in selections
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
        new_selection = SelectionService.add_empty_selection(name=name)
        return jsonify({"selection": SelectionPresenter.summary(new_selection)}), 201
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
    exists = SelectionService.selection_exists(name)
    if exists:
        return jsonify({"valid": False, "error": "Name already exists."}), 200
    return jsonify({"valid": True}), 200

@bp.route("/<sname>", methods=["GET"])
def get_selection(sname: str):
    """ Get a specific selection by id. """
    try:
        selection = SelectionService.get_selection(sname)
        payload = {"selection": SelectionPresenter.detailed(selection)}
        return render_template("selection_detail.html", **payload), 200
    except KeyError:
        logger.warning(f"Selection not found: {sname}")
        return jsonify({"error": "Selection not found"}), 404
    except Exception:
        logger.exception(f"Error retrieving selection: {sname}")
        return jsonify({"error": "Internal server error"}), 500  

@bp.route("/<sname>", methods=["PUT"])
def update_selection(sname: str):
    """ Update a specific selection by id. """
    return jsonify({"message": "Not implemented"}), 501

@bp.route("/<sname>", methods=["DELETE"])
def delete_selection(sname: str):
    """ Delete a specific selection by id. """
    try:
        SelectionService.remove_selection(sname)
        return "", 204
    except KeyError:
        logger.warning(f"Selection not found for deletion: {sname}")
        return jsonify({"error": "Selection not found"}), 404
    except Exception:
        logger.exception(f"Error deleting selection: {sname}")
        return jsonify({"error": "Internal server error"}), 500

@bp.route("/<sname>/clone", methods=["POST"])
def clone_selection(sname: str):
    """ Clone a specific selection by id. """
    data = request.get_json(silent=True)
    new_name = data.get("name") if data else None
    if not new_name:
        return jsonify({"error": "New selection name is required for cloning"}), 400
    try:
        cloned_selection = SelectionService.clone_selection(sname, new_name=new_name)
        return jsonify({"selection": SelectionPresenter.summary(cloned_selection)}), 201
    except KeyError:
        logger.warning(f"Selection not found for cloning: {sname}")
        return jsonify({"error": "Selection not found"}), 404
    except Exception:
        logger.exception(f"Error cloning selection: {sname}")
        return jsonify({"error": "Internal server error"}), 500
