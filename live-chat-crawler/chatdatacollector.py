#! ./.venv/bin/python
# -*- coding: utf-8 -*-

# ---standard library---
import asyncio
import concurrent.futures
from datetime import datetime, timedelta
import logging
from logging import DEBUG, INFO, Logger, getLogger
import json
import csv
import time

# ---third party library---
import yt_dlp

# ---local library---
from sql_connect import DatabaseConnect
from youtubeapi import YoutubeApi
from chatdatamodule import ChatDataModule
import property

class ChatDataCollector():
    def __init__(self) -> None:
        self.logger = getLogger(__name__)
        self.videolist_table = 'videodata'
        self.chatdata_table = 'chatdata'
        self.ytdl_ops = {'cookiefile':'cookie/cookies.txt'}

    def get_videolist(self, channel_id):
        # Youtube-DLから動画情報を取得
        for i in range(5): # タイムアウト等を考慮し、5回まで取得に挑戦する
            try:
                info = yt_dlp.YoutubeDL(self.ytdl_ops).extract_info(f'https://www.youtube.com/channel/{channel_id}', download=False)
                self.logger.debug(json.dumps(info, indent=4))
                break
            except yt_dlp.utils.DownloadError as e: #チャンネルURLが有効でない場合にエラーを返す
                self.logger.error(e)
            except yt_dlp.utils.ExtractorError as e: #動画の抽出に失敗した場合は待機するため処理を続行する
                self.logger.error(e)
                if 'This live event will begin in' in str(info.args) or 'Premiere' in str(info.args):
                    return
            except Exception as e:
                self.logger.error(e)
            time.sleep(10)

        for video in info['entries']:
            with DatabaseConnect('livechat_database') as db:
                try:
                    #ボトルネック
                    db.cursor.execute(f'SELECT COUNT(id) FROM {self.videolist_table} WHERE id = \"{video.get("id")}\";')
                except Exception as e:
                    raise e
                if db.cursor.fetchone()[0] != 0:
                    self.logger.info(f'{video.get("id")} skip! already getted info.')
                    continue

            if video['is_live'] == 'True':
                self.logger.info(f'{video.get("id")} skip! video has live now. ')
                continue

            self.logger.info(f'get {video.get("id")} info start')

            title       = video.get('fulltitle')
            description = video.get('description')
            timestamp   = datetime.fromtimestamp(int(video.get('release_timestamp'))) if video.get('release_timestamp') != None else datetime.strptime(video.get('upload_date') , '%Y%m%d')
            is_live     = video.get('was_live')
            live_chat   = 'live_chat' in video.get('subtitles').keys() if video.get('subtitles') != None else False

            sql = f'INSERT INTO {self.videolist_table} VALUES (%s, %s, %s, %s, %s, %s, %s)'
            with DatabaseConnect('livechat_database') as db:
                try:
                    db.cursor.execute(sql,(video.get('id'), channel_id, title, description, timestamp.strftime('%Y-%m-%d %H:%M:%S'), is_live, live_chat))
                except Exception as e:
                    raise e

    def update_videolist(self, channel_id):
        ytm = YoutubeApi()
        video_id_lists = ytm.get_video_list(channel_id, maxResults=50)
        for video_id in video_id_lists:
            with DatabaseConnect('livechat_database') as db:
                try:
                    #ボトルネック
                    db.cursor.execute(f'SELECT COUNT(id) FROM {self.videolist_table} WHERE id = \"{video_id}\";')
                except Exception as e:
                    raise e
                if db.cursor.fetchone()[0] != 0:
                    self.logger.info(f'{video_id} skip! already getted info.')
                    continue
            # Youtube-DLから動画情報を取得
            for i in range(5): # タイムアウト等を考慮し、5回まで取得に挑戦する
                try:
                    info = yt_dlp.YoutubeDL().extract_info(f'https://youtu.be/{video_id}', download=False)
                    self.logger.debug(json.dumps(info, indent=4))
                    break
                except yt_dlp.utils.DownloadError as e: #動画URLが有効でない場合にエラーを返す
                    self.logger.error(e)
                    return
                except yt_dlp.utils.ExtractorError as e: #動画の抽出に失敗した場合は待機するため処理を続行する
                    self.logger.error(e)
                    if 'This live event will begin in' in str(info.args) or 'Premiere' in str(info.args):
                        return
                except Exception as e:
                    self.logger.error(e)
                time.sleep(10)

            if info['is_live'] == 'True':
                self.logger.info(f'{info.get("id")} skip! video has live now. ')
                continue

            title       = info.get('fulltitle')
            description = info.get('description')
            timestamp   = datetime.fromtimestamp(int(info.get('release_timestamp'))) if not info.get('release_timestamp') is None else datetime.strptime(info.get('upload_date') , '%Y%m%d')
            is_live     = info.get('was_live')
            live_chat   = 'live_chat' in info.get('subtitles').keys() if info.get('subtitles') != None else False

            sql = f'INSERT INTO {self.videolist_table} VALUES (%s, %s, %s, %s, %s, %s, %s)'
            with DatabaseConnect('livechat_database') as db:
                try:
                    db.cursor.execute(sql,(info.get('id'), channel_id, title, description, timestamp.strftime('%Y-%m-%d %H:%M:%S'), is_live, live_chat))
                except Exception as e:
                    raise e

    async def create_chatdatabase(self):
        with DatabaseConnect('livechat_database') as db:
            try:
                db.cursor.execute(
                f'select id from {self.videolist_table}\
                    WHERE not exists (\
                        SELECT DISTINCT videoId FROM {self.chatdata_table}\
                            WHERE {self.chatdata_table}.videoId = {self.videolist_table}.id\
                    ) and {self.videolist_table}.isLive = True;'\
                )
                video_id_list = db.cursor.fetchall()
            except Exception as e:
                raise e
        cdm = ChatDataModule()
        loop = asyncio.get_running_loop()

        gather = []
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=61)
        for video_id in video_id_list:
            gather.append(loop.run_in_executor(executor, cdm.get_chatdata, video_id[0]))

        await asyncio.gather(*gather)

    async def input_channel_list(self, update=False):
        with open('input/channel_list.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(filter(lambda row: row[0]!='#', f))#, comment='#')
            channels = [row for row in reader]
        loop = asyncio.get_event_loop()
        executor = concurrent.futures.ProcessPoolExecutor(max_workers=16)
        for channel in channels:
            self.logger.info(f'{channel[0]}:{channel[1]}')
            gather = []
            if (update):
                gather.append(loop.run_in_executor(executor, self.update_videolist, channel[0]))
            else:
                gather.append(loop.run_in_executor(executor, self.get_videolist, channel[0]))

        await asyncio.gather(*gather)

if __name__ == '__main__':
    logging.basicConfig(
        level=DEBUG,
        format='[ %(levelname)-8s] %(asctime)s | %(name)-32s %(funcName)-24s| %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    fh = logging.FileHandler(filename='log/chat_data_search_system.log', encoding='utf-8')
    fh.setLevel=DEBUG
    fh.setFormatter(logging.Formatter('[ %(levelname)-8s] %(asctime)s | %(name)-32s %(funcName)-24s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

    logger = getLogger(__name__)
    logger.addHandler(fh)

    cdc = ChatDataCollector()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(cdc.input_channel_list(update=True))
    loop.run_until_complete(cdc.create_chatdatabase())
