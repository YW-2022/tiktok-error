import json
import logging
import subprocess
import sys
from datetime import datetime, timedelta
from os import getenv
from time import sleep

import pandas as pd
import requests

URL_BASE = ('https://open.tiktokapis.com/v2/research/{query}/?fields={'
            'fields}')
TESTING = getenv('SJTU_TESTING', True)


def wait_for_scheduler(schedule):
    """
    # if schedule is 'hourly', run the script every hour, wait for the
    # whole hour to start the script for the 1st time, then run the script
    # every hour
    :param schedule:
    :return:
    """
    # get current date and time
    now = datetime.now()
    # convert the difference to seconds
    delta = 0

    if schedule == 'hourly':
        # next whole hour from now
        next_hour = now.replace(microsecond=0, second=0, minute=0) + timedelta(hours=1)
        # get the difference between the current time and the next hour
        delta = next_hour - now

    # if schedule is 'daily', run the script every day, wait for the whole
    # day to start the script for the 1st time, then run the script every day
    elif schedule == 'daily':
        next_day_date = now + timedelta(days=1)
        next_day_dt = next_day_date.replace(microsecond=0, second=0, minute=0, hour=0)
        delta = next_day_dt - now
    # convert the difference to seconds
    delta_seconds = delta.total_seconds()
    # sleep for the difference in seconds
    if not TESTING:
        logging.info(f"Sleeping for {delta_seconds} seconds")
        sleep(delta_seconds)


class JSONFormatter(logging.Formatter):
    def format(self, record):
        # log_record = {'time': self.formatTime(record, self.datefmt),
        #               'level': record.levelname,
        #               'message': record.getMessage(),
        #               'name': record.name,
        #               'filename': record.filename,
        #               'lineno': record.lineno,
        #               'funcName': record.funcName, }
        log_record = {'t': self.formatTime(record, self.datefmt),
                      'msg': record.getMessage(), 'f': record.filename,
                      'func': record.funcName, 'l': record.lineno, }
        return json.dumps(log_record)


def setup_logger(name, log_file, level=logging.INFO):
    # formatter_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    # formatter = logging.Formatter(formatter_str)
    # logging.basicConfig(stream=sys.stdout, level=logging.INFO,
    #                     format=formatter_str)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.FileHandler(log_file)
    handler.setLevel(level)
    formatter = JSONFormatter()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def get_available_logger():
    logger_dict = logging.root.manager.loggerDict
    available_loggers = [name for name in logger_dict if
                         isinstance(logger_dict[name], logging.Logger)]
    # search for logger names that starts with sjtu
    sjtu_loggers = [name for name in available_loggers if
                    name.startswith('sjtu')]
    assert len(
        sjtu_loggers) == 1, 'There should be only one logger that starts with sjtu'
    return sjtu_loggers[0]


# function that returns the access token
def get_access_token():
    """
    This function returns the access token
    :return: access token
    """
    url = 'https://open.tiktokapis.com/v2/oauth/token/'
    headers = {'Content-Type': 'application/x-www-form-urlencoded',
               'Cache-Control': 'no-cache', }

    data = {'client_key': 'awjujzjm3pne4fcy',
            'client_secret': 'H2EYfH2TwgOowPJP93cjM3ZGx8qKG6Ns',
            'grant_type': 'client_credentials', }

    response = requests.post(url, headers=headers, data=data)

    json_data = json.loads(response.text)
    return json_data['access_token']


# function that returns the videos
def get_videos(token, query):
    """
    This function returns the videos
    :param token:
    :return: a dataframe of videos, each containing id, like_count, username i.e. the
    fields specified in url_video_fields
    """
    video = get_video_list(token, query)
    df_videos = video_list_to_df(video)
    return df_videos


# function that returns the videos
def get_video_list(token, query):
    """
    This function returns the videos in the format of list, instead of
    dataframe.
    this is because Tiktok API has a limit of 30 days between start and end,
    and list is easier to concatenate.
    :param token:
    :return: a dataframe of videos, each containing id, like_count, username i.e. the
    fields specified in url_video_fields
    """
    # start logging, using append mode
    logger = logging.getLogger(get_available_logger())

    url_video_fields = ("id,video_description,create_time, region_code,"
                        "share_count,view_count,like_count,comment_count, "
                        "music_id,hashtag_names, username,effect_ids,"
                        "playlist_id,voice_to_text")
    url_video = URL_BASE.format(query='video/query', fields=url_video_fields)
    max_count = 100

    headers = {'authorization': 'bearer ' + token}
    data = query
    data["max_count"] = max_count

    # the `json=data` flag is important, it generate a request with content type
    # set to application/json
    # the `data=data` flag will generate a request with content type set to
    # application/x-www-form-urlencoded and the data will be encoded in the body
    # of the request
    # https://stackoverflow.com/questions/26685248/difference-between-data-and-json-parameters-in-python-requests-package
    video = []
    j = 1
    while True:
        # try to load the response as json, if it fails, wait for 2 second and
        # keep trying again until it succeeds
        logger.info('~~~new page'
                    '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        logger.info(f'try to load page No. {j}.')
        q = 1
        while True:
            try:
                if TESTING:
                    # empty json response for testing
                    response = {}
                else:
                    response = requests.post(url_video, headers=headers,
                                             json=data)

                resp_data = json.loads(response.text)
                logger.info(json.dumps({'query': data}))
                logger.info(json.dumps({'response': resp_data}))

                if resp_data['error']['code'] != "ok":
                    if resp_data['error'][
                            'code'] == "daily_quota_limit_exceeded":
                        logger.warning('daily quota limit exceeded, EXIT')
                    if resp_data['error']['code'] == "invalid_token":
                        logger.warning('invalid token, EXIT')
                    logger.warning('unknown error, EXIT')
                    return video

                video.extend(resp_data['data']['videos'])
                has_more = resp_data['data']['has_more']
                if has_more:
                    search_id = resp_data['data']['search_id']
                    cursor = resp_data['data']['cursor']

                # if the current page load is successful, break the loop and
                # move to the next page
                break
            except:
                sleep(1)
                logger.info("Data not ready. No. wait: " + str(q))
                q += 1

        j += 1
        # print('getting videos, times: ', j)
        # print(resp_data)
        # logger.info('getting videos, times: ' + str(j))

        if has_more:
            data['search_id'] = search_id
            data['cursor'] = cursor
        else:
            # if there is no more for the current query, return to the caller
            break

        # pause for 30 second  # sleep(1)  # print("pause for 1 second")  # logger.info("pause for 1 second")

    return video


# function that convert list of videos to dataframe, add url column

def video_list_to_df(video_list):
    df_videos = pd.DataFrame(video_list)
    df_videos = df_videos.drop_duplicates(subset=['id'])
    # string concatenate two columns username and id to get the url and save it in a new column
    df_videos['url'] = df_videos.apply(
        lambda row: 'https://www.tiktok.com/@' + row['username'] + '/video/' + str(row['id']), axis=1)
    return df_videos


def get_user_info(token, users):
    """
    :param token: token to access the api
    :param users: dataframe series of usernames
    :return:
    """

    logger = logging.getLogger(get_available_logger())
    logger.info('inside get_user_info')

    url_user_fields = ("display_name,bio_description,avatar_url,is_verified,"
                       "follower_count,following_count,likes_count,video_count")
    url_user = URL_BASE.format(query='user/info', fields=url_user_fields)

    headers = {'authorization': 'bearer ' + token,
               'Content-Type': 'application/json'}

    user_info = []
    j = 0
    for user in users:
        data = {"username": user}

        i = 0
        while True:
            try:
                response = requests.post(url_user, headers=headers, json=data)
                resp_data = json.loads(response.text)
                data_dict = resp_data['data']
                data_dict["username"] = user
                logger.info(json.dumps(data))
                logger.info(json.dumps(resp_data))
                if resp_data['error']['code'] == "daily_quota_limit_exceeded":
                    logger.info('daily quota limit exceeded, EXIT')
                    # exit the program
                    return pd.DataFrame(user_info)
                user_info.append(data_dict)
                break
            except:
                sleep(2)
                i += 1
                logger.info("data not ready, waiting for 2 second")
                logger.info('waiting for data, times: ' + str(i))
                if i > 5:
                    logger.info('for current user: ' + str(user))
                    logger.info('no data in response after 5 times, break')
                    error_user = {"username": user, "error": "no data in "
                                                             "response after 5 times"}
                    user_info.append(error_user)
                    break

        j += 1
        logger.info('current user: ' + user)
        logger.info('user position: ' + str(j) + ' of ' + str(len(users)))

        sleep(1)
        logger.info("pause for 1 second")

    return pd.DataFrame(user_info)


def get_comments(token, ids):
    """
    This function returns the comments of the videos
    :param token: token to access the api
    :param ids: dataframe series of video ids
    :return: dataframe of comments
    """

    logger = logging.getLogger(get_available_logger())
    logger.info('inside get_comments')

    url_comment_fields = ("id,video_id,text,like_count,reply_count,"
                          "parent_comment_id,create_time")
    url_comment = URL_BASE.format(query='video/comment/list',
                                  fields=url_comment_fields)
    max_count = 100

    headers = {'authorization': 'bearer ' + token,
               'Content-Type': 'application/json'}

    comments = []
    j = 0
    for vid in ids:
        data = {"video_id": vid, "max_count": max_count}

        while True:
            # while data is not in response, wait for 30 seconds and try the
            # same request again until data is in response
            i = 0
            try:
                response = requests.post(url_comment, headers=headers,
                                         json=data)
                resp_data = json.loads(response.text)
                comments.extend(resp_data['data']['comments'])
                has_more = resp_data['data']['has_more']
                if has_more:
                    cursor = resp_data['data']['cursor']
                logger.info(json.dumps(data))
                logger.info(json.dumps(resp_data))
                break
            except:
                sleep(2)
                i += 1
                # logger.info("data not ready, waiting for 2 second")
                logger.info('waiting for data, times: ' + str(i))
                if i > 5:
                    # logger.info('for current video: ' + str(vid))
                    logger.info('no data in response after 5 times, break')
                    break

            # check if data exists in the response
            if has_more:
                data['cursor'] = cursor
            else:
                break

        j += 1
        logger.info('above for video: ' + str(vid))
        logger.info('video position: ' + str(j) + ' of ' + str(len(ids)))
        logger.info('###################################################')

        # sleep(1)  # logger.info("pause for 1 second")

    return pd.DataFrame(comments)


def download_videos(video_ids, output_folder):
    for vd in video_ids:
        print(vd)
        subprocess.run(['yt-dlp', f'https://www.tiktok.com/@/video/{vd}', '-o',
                        f'{output_folder}/{vd}.mp4'])
        # sleep(1)

        print("pause for 1 second")


if __name__ == '__main__':
    # create logger
    logger = setup_logger('default_logger', 'log_default.log')
    formatter_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                        format=formatter_str)
    token = get_access_token()
    query = {"query": {"and": [{"operation": "IN", "field_name": "region_code",
                                "field_values": ["GB"]},
                               {"operation": "EQ", "field_name": "keyword",
                                "field_values": ["hello world"]}]},
             "start_date": "20220615", "end_date": "20220628"}
    df_videos = get_videos(token, query)  # this gives a dataframe of 35 videos
    df_videos.to_csv('test_videos.csv', index=False)  # save as csv file
    df_users = get_user_info(token, df_videos['username'].drop_duplicates())
    df_users.to_csv('test_users.csv', index=False)  # save as csv file
    df_comments = get_comments(token, df_videos['id'].drop_duplicates())
    df_comments.to_csv('test_comments.csv', index=False)  # save as csv file  #
