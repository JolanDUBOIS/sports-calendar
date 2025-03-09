from src.main import run_selection, test
from src.cli_utils import parse_arguments
from src.scraper_db import update_database


if __name__ == '__main__':
    args = parse_arguments()

    if args.update_database:
        update_database()
    
    elif args.full_update:
        pass

    elif args.run_selection:
        run_selection(save_ics=True)

    elif args.test:
        test(args.test)
