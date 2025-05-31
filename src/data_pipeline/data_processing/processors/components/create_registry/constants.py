# Teams

GENERIC_TEAM_TOKENS = [
    "FC", "Football Club", "Soccer Club", "SC", "Sporting Club",
    "AC", "Club", "Associazione Calcio", "SV", "Sportverein",
    "SS", "Società Sportiva", "CF", "Club de Fútbol", "SK", "Sportklub",
    "US", "Unione Sportiva", "Union Sportive", "CD", "Club Deportivo", "AFC",
    "Association Football Club", "AS", "Associazione Sportiva", "Association Sportive",
]

TEAM_REGISTRY_PARAMETERS = {
    "espn_teams": {
        "column_variants": ["team_name", "team_displayName", "team_shortDisplayName"], #, "team_abbreviation" # Careful with the abbreviation
        "id_col": "team_id",
    },
    "live_soccer_teams": {
        "column_variants": ["team_name"],
        "id_col": "team_name",
    },
    "football_data_teams": {
        "column_variants": ["team_name", "team_shortName"], #, "tla"], # Careful with the TLA
        "id_col": "team_id",
    }
}

# Competitions

GENERIC_COMPETITION_TOKENS = []

COMPETITION_REGISTRY_PARAMETERS = {
    "espn_competitions": {
        "column_variants": ["competition_name", "competition_abbreviation"],
        "id_col": "competition_id",
    },
    "live_soccer_competitions": {
        "column_variants": ["competition"],
        "id_col": "competition",
    },
    "football_data_competitions": {
        "column_variants": ["competition_name"],
        "id_col": "competition_id",
    }
}

# Areas

GENERIC_AREA_TOKENS = []

AREA_REGISTRY_PARAMETERS = {
    "live_soccer_areas": {
        "column_variants": ["area"],
        "id_col": "area",
    },
    "football_data_areas": {
        "column_variants": ["area_name"],
        "id_col": "area_id",
    }
}

# Constants

REGISTRY_CONSTANTS = {
    "teams": {
        "generic_tokens": GENERIC_TEAM_TOKENS,
        "registry_parameters": TEAM_REGISTRY_PARAMETERS,
    },
    "competitions": {
        "generic_tokens": GENERIC_COMPETITION_TOKENS,
        "registry_parameters": COMPETITION_REGISTRY_PARAMETERS,
    },
    "areas": {
        "generic_tokens": GENERIC_AREA_TOKENS,
        "registry_parameters": AREA_REGISTRY_PARAMETERS,
    }
}