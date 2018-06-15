import argparse
import logging
import os
import sys
from db_util import db_manip as db
import util.loggerinitializer as utl

# Initialize log object
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
utl.initialize_logger(os.getcwd(), logger)


def main():
    parser = argparse.ArgumentParser(description="A Tool manipulate a sqlite DB")

    subparsers = parser.add_subparsers(title='actions',
                                       description='valid actions',
                                       help='Use sqlite-python.py {action} -h for help with each action',
                                       dest='command'
                                       )
    #   CREATE DB
    parser_index = subparsers.add_parser('createdb', help='Create database and tables')

    parser_index.add_argument("--db", dest='db', default=None, action="store", help="The DB name",
                        required=True)
    #   INSERT DB
    parser_insert = subparsers.add_parser('insert', help='Insert data on tables')

    parser_insert.add_argument("--file",  default=None, action="store", help="CSV file with the data to be inserted",
                        required=True)

    parser_insert.add_argument("--db", default=None, action="store", help="The DB name",
                        required=True)

    #   UPDATE DB
    parser_update = subparsers.add_parser('update', help='Update a field in a db')

    parser_update.add_argument("--db", default=None, action="store", help="The DB name",
                        required=True)

    parser_update.add_argument("--assay", default=None, action="store", help="Name of the project's author",
                        required=True)

    parser_update.add_argument("--donor", default=None, action="store", help="status",
                        required=False)

    #   SELECT DB
    parser_select = subparsers.add_parser('select', help='Select  fields from the db')

    parser_select.add_argument("--db", default=None, action="store", help="The DB name",
                        required=True)

    parser_select.add_argument("-a", action="store_true", help="Select all Project data",
                        required=False)

    parser_select.add_argument("-tn", action="store", help="Select information about track name belonging to an specific assay track name",
                        required=False)

    parser_select.add_argument("-aia", action="store",
                               help="Select all information belonging to an specific assay",
                               required=False)

    parser_select.add_argument("-ct", action="store",
                               help="Select information about cell_type belonging to an specific assay",
                               required=False)
    #   DELETE DB
    parser_delete = subparsers.add_parser('delete', help='delete rows from the db')

    parser_delete.add_argument("-track_name", default=None, action="store", help="Delete rows where this author appears",
                        required=False)

    parser_delete.add_argument("--db", default=None, action="store", help="The DB name",
                        required=True)


    args = parser.parse_args()
    conn = db.connect_db(args.db, logger)

    if args.command == "createdb":
        db.create_table(conn, logger)

    elif args.command == "insert":
        db.insert_data(conn, args.file, logger)

    elif args.command == "update":
        db.update_status(conn, args.assay, args.donor, logger)

    elif args.command == "select" and args.a is not False:
        all_proj = db.select_all(conn, logger)
        print(all_proj)

    elif args.command == "select" and args.tn is not False:
        all_status = db.select_track_name(conn, args.tn, logger)
        print(all_status)

    elif args.command == "select" and args.ct is not False:
        all_status = db.select_cell_type(conn, args.ct, logger)
        print(all_status)

    elif args.command == "select" and args.aia is not False:
        all_status = db.select_(conn, args.aia, logger)
        print(all_status)

    elif args.command == "delete":
        db.delete_author(conn, args.track_name, logger)

if __name__ == '__main__':
    main()
