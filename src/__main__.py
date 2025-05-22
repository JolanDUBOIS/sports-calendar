from dotenv import load_dotenv

from src.main import test
from src.config import parse_arguments


load_dotenv()

if __name__ == '__main__':
    args = parse_arguments()

    if args.update_database:
        raise NotImplementedError("Database update is not implemented yet.")

    elif args.full_update:
        raise NotImplementedError("Full update is not implemented yet.")

    elif args.run_selection:
        raise NotImplementedError("Run selection is not implemented yet.")

    elif args.test:
        test(args.test)
