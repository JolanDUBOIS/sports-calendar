from src.main import run_selection, test
from src.config import parse_arguments, AppConfig, EnvConfig
from src.scraper_db import update_database


if __name__ == '__main__':
    args = parse_arguments()
    app_config = AppConfig(args.config_file)
    env_config = EnvConfig()

    if args.update_database:
        update_database()
    
    elif args.full_update:
        pass

    elif args.run_selection:
        run_selection(
            app_config.selection_file_path,
            app_config.google_credentials_file_path,
            env_config.google_calendar_id,
            save_ics=True
        )

    elif args.test:
        test(args.test)
