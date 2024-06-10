"""
This script is call using the following command:
python dl_video_info_schedule.py YAML_FILE test
where YAML_FILE is the yaml file to be processed
and test is an optional argument to determine if the script is being tested
'test' can be any string, if it is present, the script will run in testing mode
"""
import copy
import json
import logging
import sys
from datetime import timedelta, datetime
from itertools import combinations, product
from os import environ

import yaml

from utilities import (get_access_token, setup_logger, get_video_list,
                       video_list_to_df, get_available_logger,
                       wait_for_scheduler)

YAML_FILE = sys.argv[1] if len(sys.argv) >= 2 else None
# if YAML_FILE is not provided, the script will exit
if not YAML_FILE:
    print('Please provide the yaml file as the first argument')
    sys.exit(1)
TESTING = sys.argv[2] if len(sys.argv) == 3 else False
if TESTING:
    environ['SJTU_TESTING'] = 'True'

MSG_TEMPLATE = ("{separator}" * 3 + " {key} " + "{position} message " + (
    "{separator}") * 10)
MSG_START = MSG_TEMPLATE.format(separator='{separator}', key='{key}',
                                position='start')
MSG_END = MSG_TEMPLATE.format(separator='{separator}', key='{key}',
                              position='end')


def load_yaml_file(yaml_file):
    try:
        with open(yaml_file, 'r') as file:
            data = yaml.safe_load(file)
        return data
    except FileNotFoundError:
        print(f"YAML file {yaml_file} not found.")
        sys.exit(1)
    except yaml.YAMLError as error:
        # produce system error message, no need to log
        print(f"Error parsing YAML file {yaml_file}: {error}")
        sys.exit(1)


def get_videos(query_bases, d_start, d_end, increment):
    videos = []
    d_str = lambda d: (d.strftime("%Y") + d.strftime("%m") + d.strftime("%d"))
    intervals = int((d_end - d_start).days / increment)
    # ceil division
    if (d_end - d_start).days % increment != 0:
        intervals += 1

    token = get_access_token()
    logger = logging.getLogger(get_available_logger())

    for ind, query_bs in enumerate(query_bases):
        logger.info(MSG_START.format(separator=':', key='subquery'))
        logger.info(
            'getting videos for subquery ' + str(ind + 1) + ' of ' + str(
                len(query_bases)))
        logger.info(json.dumps({'subquery': query_bs}, default=str))
        logger.info(MSG_START.format(separator=':', key='subquery'))
        d_start_bs = copy.deepcopy(d_start)
        # d_end_bs = copy.deepcopy(d_end)
        i = 1
        while d_start_bs < d_end:
            query = copy.deepcopy(query_bs)
            query["start_date"] = d_str(d_start_bs)
            d_end_bs = copy.deepcopy(d_end) if d_end < d_start_bs + timedelta(
                days=increment) else d_start_bs + timedelta(days=increment)
            query["end_date"] = d_str(d_end_bs)
            logger.info(MSG_START.format(separator='-', key='query window'))
            logger.info(
                'getting videos for time window ' + str(i) + ' of ' + str(
                    intervals))
            logger.info('start date: ' + str(d_start_bs))
            logger.info('end date: ' + str(d_end_bs))
            logger.info(MSG_START.format(separator='-', key='query window'))
            videos.extend(get_video_list(token, query))
            logger.info(MSG_END.format(separator='-', key='query window'))
            logger.info(
                'done getting videos for time window ' + str(i) + ' of ' + str(
                    intervals))
            if len(videos) == 0:
                logger.info('no videos for this time window')
            else:
                logger.info('total number of videos after this time '
                            'window: ' + str(len(videos)))
            logger.info(MSG_END.format(separator='-', key='query window'))
            d_start_bs = copy.deepcopy(d_end_bs)
            i += 1
        logger.info(MSG_END.format(separator=':', key='subquery'))
        logger.info(
            'done getting videos for subquery ' + str(ind + 1) + ' of ' + str(
                len(query_bases)))
        logger.info(MSG_END.format(separator=':', key='subquery'))
    return videos


def main():
    if not YAML_FILE:
        print('Please provide the yaml file as the first argument')
        sys.exit(1)
    else:

        data = load_yaml_file(YAML_FILE)
        # retrieve the different variables from the loaded data and assign them
        # to variables
        out_base = data.get('out_base')
        d_start = data.get('d_start')
        d_end = data.get('d_end')
        increment = data.get('increment')
        field_names = data.get('field_names')
        field_values = data.get('field_values')
        schedule = data.get('schedule')
        total = data.get('total')

        query_bases = []
        # if field_names contain only one field_name, merge all
        # keywords into a list of strings
        if len(field_names) == 1:
            field_values = [field_value for field_value in field_values]
            query_base = {"query": {"and": [
                {"operation": "IN", "field_name": field_names[0],
                 "field_values": field_values}]}}
            query_bases.append(query_base)
        else:
            try:
                assert len(field_names) == 2 and len(field_values) == 2
                import itertools
                field_name_prod = list(product(field_names, repeat=2))
                keyword_comb = list(combinations(field_values, 2))
                comb_result = list(product(field_name_prod, keyword_comb))
                for comb in comb_result:
                    query_base = {"query": {"and": [
                        {"operation": "IN", "field_name": comb[0][0],
                         "field_values": comb[1][0]},
                        {"operation": "IN", "field_name": comb[0][1],
                         "field_values": comb[1][1]}]}}
                    query_bases.append(query_base)
            except:
                print('Error in the field_names and keywords')

        # run the script for 'total' number of times
        for i in range(total):
            if i > 0 and not TESTING:
                # wait for the scheduler to run the script again
                wait_for_scheduler(schedule)
            # get current date and time and convert to string format
            now = datetime.now()
            now_str = now.strftime("%Y%m%d_%H%M%S")
            # get current file name remove the path and the extension
            # consider both windows and linux paths
            if "\\" in __file__:
                f_name = __file__.split("\\")[-1].split(".")[0]
            else:
                f_name = __file__.split("/")[-1].split(".")[0]

            logger = setup_logger(f'sjtu_{f_name}_{out_base}',
                                  f'{out_base}_{now_str}.log')
            logger.info(MSG_START.format(separator='#', key='YAML'))
            logger.info(f"YAML file {YAML_FILE}  started @ {now_str}  ")
            # convert data dictionary to json format, no need to log
            # https://stackoverflow.com/questions/11875770/how-can-i-overcome-datetime-datetime-not-json-serializable
            # non json serializable objects should be converted to string
            logger.info(json.dumps({'yaml': data}, default=str))
            logger.info(MSG_START.format(separator='#', key='YAML'))
            videos = get_videos(query_bases, d_start, d_end, increment)
            df_videos = video_list_to_df(videos)
            df_videos = df_videos.drop_duplicates(subset=['id'])
            df_videos.to_csv(f'{out_base}_{now_str}.csv', index=False, mode='a')
            logger.info(MSG_END.format(separator='#', key='YAML'))
            logger.info("YAML file processed successfully: " + YAML_FILE)
            logger.info(MSG_END.format(separator='#', key='YAML'))


if __name__ == "__main__":
    main()
