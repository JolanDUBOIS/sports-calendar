from flask import request, jsonify, Blueprint

from ..presenters import LookupPresenter


bp = Blueprint("lookups", __name__, url_prefix="/lookups")

@bp.route("/sports", methods=["GET"])
def get_sports():
    """ TODO """
    return jsonify({"message": "Not implemented"}), 501

@bp.route("/<sport>/teams", methods=["GET"])
def get_teams(sport: str):
    query = request.args.get("query", "")  # GET param instead of JSON
    payload = LookupPresenter.teams(sport, query)
    return jsonify(payload), 200

@bp.route("/<sport>/competitions", methods=["GET"])
def get_competitions(sport: str):
    query = request.args.get("query", "")
    payload = LookupPresenter.competitions(sport, query)
    return jsonify(payload), 200
