from flask import Blueprint


bp = Blueprint("items", __name__, url_prefix="/selections/<sid>/items")

@bp.route("/", methods=["POST"])
def create_item(sid: str):
    pass

@bp.route("/<iid>", methods=["GET"])
def get_item(sid: str, iid: str):
    pass

@bp.route("/<iid>", methods=["PUT"])
def update_item(sid: str, iid: str):
    pass

@bp.route("/<iid>", methods=["DELETE"])
def delete_item(sid: str, iid: str):
    pass

@bp.route("/<iid>/clone", methods=["POST"])
def clone_item(sid: str, iid: str):
    pass
