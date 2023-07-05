#!/usr/bin/env python

""" word_analysis.py: extracts biological terms from EGA study, dataset and dac title and descriptions """

__author__ = "Mauricio Moldes"
__version__ = "0.1"
__maintainer__ = "Mauricio Moldes"
__email__ = "mauricio.moldes@crg.eu"
__status__ = "Developement"

import logging
import sys
import psycopg2
import yaml
import csv
import re


logger = logging.getLogger('dac_gdpr_logger')

""" VERIFIES THE CONNECTION TO PLSQL """


def connection_plsql(cfg):
    plsql_conn_string = "host='" + str(cfg['plsql_db']['host']) + "' dbname='" + str(
        cfg['plsql_db']['dbname']) + "' user='" + str(cfg['plsql_db']['user']) + "' password='" + str(cfg['plsql_db'][
                                                                                                          'password']) + "' port = '" + str(
        cfg['plsql_db']['port']) + "'"
    conn_plsql = psycopg2.connect(plsql_conn_string)
    return conn_plsql


def get_title_data(conn_plsql, metadata_object):
    cursor = conn_plsql.cursor()
    sql = "select title from {}_table"
    cursor.execute(sql.format(metadata_object))
    records = cursor.fetchall()
    return records


def get_study_title_data(conn_plsql):
    cursor = conn_plsql.cursor()
    cursor.execute(
        "select title from study_table where repository = 'EGA' and final_release_status = 'RELEASED'  ")
    records = cursor.fetchall()
    return records


def get_description_data(conn_plsql, metadata_object):
    cursor = conn_plsql.cursor()
    sql = "select description from {}_table "
    cursor.execute(sql.format(metadata_object))
    records = cursor.fetchall()
    return records


def get_study_description_data(conn_plsql):
    cursor = conn_plsql.cursor()
    cursor.execute(
        "select description study_table from study_table st where repository = 'EGA'")
    records = cursor.fetchall()
    return records


""" QUERY STUDY DATA """


def get_study_data(conn_plsql):
    cursor = conn_plsql.cursor()
    cursor.execute(
        "select ega_stable_id , title , description from study_table st where repository = 'EGA'")
    records = cursor.fetchall()
    return records


""" QUERY DATASET DATA """


def get_dataset_data(conn_plsql):
    cursor = conn_plsql.cursor()
    cursor.execute(
        "select ega_stable_id, title ,description from dataset_table dt")
    records = cursor.fetchall()
    return records


""" QUERY DAC DATA """


def get_dac_data(conn_plsql):
    cursor = conn_plsql.cursor()
    cursor.execute(
        "select ega_stable_id ,title ,description from dac_table dact")
    records = cursor.fetchall()
    return records


""" DUMP DATA """


def dump_data(file_name, data):
    with open("../data/" + file_name + ".txt", "a+", newline='') as output:
        output.write(str(data))


""" HARDCODED STOPWORDS """


def read_stopwords():
    file1 = open('../data/stopwords.txt', 'r')
    stopwords = file1.readlines()
    stopwords_converted_list = []
    for element in stopwords:
        stopwords_converted_list.append(element.strip())
    return stopwords_converted_list


""" READ INPUT FILE """


def read_data(file_name):
    file = open("../data/" + file_name + ".txt", encoding="utf8")
    a = file.read()
    target = remove_non_alphanumeric(a)
    target_split = target.split()
    target_lower = [x.lower() for x in target_split]
    str1 = ' '.join(target_lower)
    stopword_lower
    removed = remove_stop_words(str1, read_stopwords())
    wordlist = removed.split()
    from collections import Counter
    counts = Counter(wordlist)
    for key, value in counts.items():
        count_file = open("../data/" + file_name + "_word_count.txt", "a+", encoding="utf8")
        count_file.write(str(key) + " " + str(value) + "\n")
    dump_data(file_name + "_counts", counts)


def remove_non_alphanumeric(source_string):
    target_string = re.sub("[^0-9a-zA-Z]+", " ", source_string)
    return target_string


def remove_stop_words(source_list, stopwords):
    for tag in stopwords:
        source_list = re.sub(r'\b' + tag + r'\b', '', source_list)
    return source_list


""" READ CONFIG FILE """


def read_config(path):
    with open(path, 'r') as stream:
        results = yaml.load(stream)
    return results


def tofu(title):
    target = remove_non_alphanumeric(title)
    target_split = target.split()
    target_lower = [x.lower() for x in target_split]
    str1 = ' '.join(target_lower)
    stopwords = read_stopwords()
    stopwords_lower = [x.lower() for x in stopwords]
    removed = remove_stop_words(str1, stopwords_lower)
    return removed


"""" DUMP  """


def word_analysis(cfg):
    conn_plsql = None
    try:
        conn_plsql = connection_plsql(cfg)  # conn to plsql
        if conn_plsql:  # has required connection
            study_results = get_study_data(conn_plsql)
            # dataset_results = get_dataset_data(conn_plsql)
            # dac_results = get_dac_data(conn_plsql)

            header = ['study_id', 'title', 'description']
            with open('../data/testito_csv.csv', 'w', encoding='UTF8', newline='') as f:
                writer = csv.writer(f)
                # write the header
                writer.writerow(header)

                for study in study_results:
                    # write multiple rows
                    title = tofu(study[1])
                    description = study[2]
                    ega_stable = study[0]
                    lele = [ega_stable, title, description]
                    writer.writerow(lele)

            for study in study_results:
                study_id = study[0]
                title = study[1]
                description = study[2]
                target = remove_non_alphanumeric(title)
                target_split = target.split()
                target_lower = [x.lower() for x in target_split]
                str1 = ' '.join(target_lower)
                stopwords = read_stopwords()
                stopwords_lower = [x.lower() for x in stopwords]
                removed = remove_stop_words(str1, stopwords_lower)
                wordlist = removed.split()
                from collections import Counter
                counts = dict(Counter(wordlist))
                for key, value in counts.items():
                    print(study_id, key, value)
                    string = str(study_id) + "  " + str(key) + "    " + str(value) + "\n"
                    dump_data("description" + "_counts", string)
                # count_file = open("../data/" + file_name + "_word_count.txt", "a+", encoding="utf8")
                # count_file.write(str(key) + " " + str(value) + "\n")

            # study_title_results = get_study_title_data(conn_plsql)
            # dataset_title_results = get_title_data(conn_plsql, "dataset")
            # dac_title_results = get_title_data(conn_plsql, "dac")
            # study_description_results = get_study_description_data(conn_plsql)
            # dataset_description_results = get_description_data(conn_plsql, "dataset")
            # dac_description_results = get_description_data(conn_plsql, "dac")

            # dump_data("study", study_results)
            # dump_data("dataset", dataset_results)
            # dump_data("dac", dac_results)

            # dump_data("study_title", study_title_results)
            # dump_data("dataset_title", dataset_title_results)
            # dump_data("dac_title", dac_title_results)

            # dump_data("study_description", study_description_results)
            # dump_data("dataset_description", dataset_description_results)
            # dump_data("dac_description", dac_description_results)

            # read_data("study")
            # read_data("study_description")
            # read_data("dataset_description")
            # read_data("dac_description")
            # read_data("study_title")
            # read_data("dataset_title")
            # read_data("dac_title")
            # read_data("dac")
            # read_data("dataset")


        else:
            logger.debug("plsql db is not available")
            logger.info("word analysis process ended")
    except psycopg2.DatabaseError as e:
        logger.warning("Error creating database:{} ".format(e))
        raise RuntimeError('Database error') from e
    finally:
        if conn_plsql:
            conn_plsql.close()
            logger.debug("plsql connection closed")


def run():
    try:
        # configure logging
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s [in %(pathname)s:%(lineno)d]'
        logging.basicConfig(format=log_format)
        # read config file
        cfg = read_config("../bin/config.yml")
        # execute main function
        word_analysis(cfg)
    except Exception as e:
        logger.error("Error: {}".format(e))
        sys.exit(-1)


if __name__ == '__main__':
    ## cue the music
    run()
