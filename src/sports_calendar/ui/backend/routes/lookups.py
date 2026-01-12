from flask import Blueprint


bp = Blueprint("lookups", __name__, url_prefix="/lookups")

@bp.route("/teams", methods=["GET"])
def get_teams():
    pass

@bp.route("/competitions", methods=["GET"])
def get_competitions():
    pass
