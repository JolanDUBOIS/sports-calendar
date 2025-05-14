from datetime import datetime, timedelta


def generate_date_range(date_from: str, date_to: str) -> list:
    """ Generate a list of dates between two dates. """
    try:
        start_date = datetime.strptime(date_from, "%Y-%m-%d")
    except ValueError:
        start_date = datetime.strptime(date_from, "%Y-%m-%d %H:%M:%S").date()
    try:
        end_date = datetime.strptime(date_to, "%Y-%m-%d")
    except ValueError:
        end_date = datetime.strptime(date_to, "%Y-%m-%d %H:%M:%S").date()
    
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    
    return date_list
