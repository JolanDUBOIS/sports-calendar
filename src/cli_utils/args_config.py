import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description='Application for sports calendar generation.')

    parser.add_argument(
        '-t',
        '--test',
        type=int,
        help='Run the indicated test.'
    )

    parser.add_argument(
        '-udb',
        '--update-database',
        action='store_true',
        default=False,
        help='Updates the databases by querying APIs. Should be run at least 24 hours before running the selection.'
    )
    
    parser.add_argument(
        '-fu',
        '--full-update',
        action='store_true',
        default=False,
        help='Erases and updates the databases by querying APIs. Should be run only by administrators.'
    )

    parser.add_argument(
        '--run-selection',
        action='store_true',
        default=False,
        help='Creates the calendar based on the provided selection.'
    )

    return parser.parse_args()
