from flask import Blueprint


bp = Blueprint("filters", __name__, url_prefix="/selections/<sid>/items/<iid>/filters")

@bp.route("/", methods=["POST"])
def create_filter(sid: str, iid: str):
    pass

@bp.route("/<filter_id>", methods=["GET"])
def get_filter(sid: str, iid: str, filter_id: str):
    pass

@bp.route("/<filter_id>", methods=["PUT"])
def update_filter(sid: str, iid: str, filter_id: str):
    pass

@bp.route("/<filter_id>", methods=["DELETE"])
def delete_filter(sid: str, iid: str, filter_id: str):
    pass
