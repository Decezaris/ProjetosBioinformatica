import sqlite3
import pandas as pd
import numpy as np


def connect_db(db_name, logger):
    try:
        conn = sqlite3.connect(db_name + '.db')
        logger.info(f'Connection stablished with DB: {db_name}.db')

        return conn


    except sqlite3.OperationalError:
        logger.error(f'Could not connect with {db_name}.db. Make sure the DB name is right')


def create_table(conn, logger):
    c = conn.cursor()

    try:
        c.execute('CREATE TABLE IF NOT EXISTS project(cell_type_category TEXT NOT NULL,'
                  'cell_type TEXT NOT NULL,  cell_type_track_name TEXT NOT NULL, '
                  'cell_type_short TEXT, assay_category TEXT NOT NULL, assay TEXT,'
                  'assay_track_name TEXT NOT NULL, assay_short TEXT NOT NULL,'
                  'donor TEXT NOT NULL, time_point INT, view TEXT NOT NULL, track_name TEXT NOT NULL,'
                  'track_type TEXT NOT NULL, track_density TEXT NOT NULL, provider_institution TEXT NOT NULL,'
                  'source_server TEXT NOT NULL, source_path_to_file TEXT NOT NULL, server TEXT NOT NULL,'
                  'path_to_file, new_file_name TEXT NOT NULL);')

        logger.info('Table project was created')

    except sqlite3.OperationalError:
        logger.error('Table project could not be created')


def insert_data(conn, file, logger):
    c = conn.cursor()
    chip_seq_template = pd.read_csv(file + '.csv')
    print(chip_seq_template.columns)
    try:
        with conn:
            rows = len(chip_seq_template)
            for line in range(rows):
                line_dict = dict(chip_seq_template.loc[line])
                if np.nan not in line_dict.values():
                    c.execute("INSERT INTO project VALUES(:cell_type_category, :cell_type, :cell_type_track_name, :cell_type_short, :assay_category, :assay, :assay_track_name, :assay_short,:donor,:time_point,:view, :track_name,:track_type,:track_density, :provider_institution, :source_server, :source_path_to_file, :server, :path_to_file, :new_file_name);",line_dict)

            logger.info('Data was inserted on the DB')

    except sqlite3.OperationalError:
        logger.error('Data could not be inserted')


def update_status(conn, assay, donor, logger):

    c = conn.cursor()

    try:
        with conn:
            c.execute("UPDATE project SET status = :status  WHERE author = :author", {'assay': assay, 'donor': donor})
            logger.info(f'Status:{assay} was updated for author: {donor}')

    except sqlite3.OperationalError:
        logger.error(f'COLD NOT UPDATE Status:{status} for author: {author}')


def select_all(conn, logger):

    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT * FROM project")
            all_data = c.fetchall()

            logger.info(f'Selected all data')
            return all_data

    except sqlite3.OperationalError:
        logger.error(f'Could not Select authors. Check if the table exists.')


def select_assay(conn, assay, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT * FROM project WHERE assay = :assay", {"status": assay})
            all_assay = c.fetchall()

            logger.info(f'Selected assay {assay}')

            return all_assay

    except sqlite3.OperationalError:
        logger.error(f'Could not Select projects. Check if the table exists.')


def select_track_name(conn, assay_track_name, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT track_name FROM project WHERE assay_track_name = :assay_track_name", {"assay_track_name": assay_track_name})
            all_status = c.fetchall()

            logger.info(f'Selected project with a assay_track_name: {assay_track_name}')

            return all_status

    except sqlite3.OperationalError:
        logger.error(f'Could not Select project statuses. Check if the table exists.')


def select_cell_type(conn, assay, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("SELECT cell_type FROM project WHERE assay = :assay", {"assay": assay})
            all_status = c.fetchall()

            logger.info(f'Selected cell_type with a assay: {assay}')

            return all_status

    except sqlite3.OperationalError:
        logger.error(f'Could not Select project statuses. Check if the table exists.')



def delete_author(conn, track_name, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("DELETE FROM project WHERE track_name = :track_name", {"track_name": track_name})

            logger.info(f'Rows where author is: "{track_name}",  were deleted')

    except sqlite3.OperationalError:
        logger.error(f'Could not delete {track_name}')