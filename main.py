import mng_args
import sys
import sql_runner
import sql_wizard
import crawler

if __name__ == "__main__":
    args = sys.argv
    args_dict = mng_args.create_parser(args)
    df = crawler.get_top_movies_data()

    sql_wizard.run()

    print('Database is ready, running the query!')
    sql_runner.run(args_dict)
