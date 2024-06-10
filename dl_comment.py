import logging
import sys

import pandas as pd

from utilities import (formatter_str, get_access_token,
                       handler_comment, get_comments)

if __name__ == '__main__':
    out_base = 'outputs'
    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                        format=formatter_str)
    logger = logging.getLogger(__name__)
    logger.handlers.clear()
    logger.addHandler(handler_comment)
    df_videos = pd.read_csv('for_comment_download.csv')
    token = get_access_token()
    df_comments = get_comments(token, df_videos['id'].drop_duplicates())
    # df_comments.to_csv(out_base + '_comments.csv', index=False)
    df_comments.to_csv('for_comment_download_comments.csv', index=False)
    logger.info('done getting comments')
