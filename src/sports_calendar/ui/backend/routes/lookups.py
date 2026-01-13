from flask import request, jsonify, Blueprint

from . import logger
from sports_calendar.core.db import SPORT_SCHEMAS


bp = Blueprint("lookups", __name__, url_prefix="/lookups/<sport>")

@bp.route("/teams", methods=["GET"])
def get_teams(sport: str):
    """ Get list of teams. """
    sport_schema  = SPORT_SCHEMAS.get(sport)
    if not sport_schema:
        logger.error(f"Sport not found: {sport}")
        return jsonify({"error": "Sport not found"}), 404
    if not sport_schema.teams:
        logger.error(f"Teams not available for sport: {sport}")
        return jsonify({"error": "Teams not available for this sport"}), 404
    view = sport_schema.teams.select("id", "name", "abbreviation", "display_name", "short_display_name", "location") # Maybe a code smell here, should add layer of abstraction... But that's on the DB's side
    teams = view.get().to_dict(orient="records")
    return jsonify({"teams": teams}), 200

@bp.route("/competitions", methods=["GET"])
def get_competitions(sport: str):
    """ Get list of competitions. """
    sport_schema  = SPORT_SCHEMAS.get(sport)
    if not sport_schema:
        logger.error(f"Sport not found: {sport}")
        return jsonify({"error": "Sport not found"}), 404
    if not sport_schema.competitions:
        logger.error(f"Competitions not available for sport: {sport}")
        return jsonify({"error": "Competitions not available for this sport"}), 404
    view = sport_schema.competitions.select("id", "name", "abbreviation", "short_name")
    competitions = view.get().to_dict(orient="records")
    return jsonify({"competitions": competitions}), 200
