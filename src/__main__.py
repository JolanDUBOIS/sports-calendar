from dotenv import load_dotenv

from src.main import run_selection, test
from src.config import parse_arguments
# from src.sources import update_database


load_dotenv()

if __name__ == '__main__':
    args = parse_arguments()

    if args.update_database:
        # update_database()
        pass
    
    elif args.full_update:
        pass

    elif args.run_selection:
        run_selection(save_ics=True)

    elif args.test:
        test(args.test)
