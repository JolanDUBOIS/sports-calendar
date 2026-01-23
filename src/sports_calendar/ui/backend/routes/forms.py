from flask import request, Blueprint, render_template

from . import logger
from sports_calendar.core.selection import SelectionService


bp = Blueprint("forms", __name__, url_prefix="/forms")

@bp.route("/filter", methods=["GET"])
def get_filter_form():
    sel_name = request.args.get("selection_name")
    item_uid = request.args.get("item_uid")
    filter_uid = request.args.get("filter_uid")
    filter_type = request.args.get("filter_type")

    if not filter_uid:
        return "Missing filter_uid", 400

    filter = SelectionService.get_filter(sel_name, item_uid, filter_uid)

    if filter_type and filter.filter_type != filter_type:
        filter = SelectionService

    return render_template(
        "_filter_form.html",
        filter=filter
    )
