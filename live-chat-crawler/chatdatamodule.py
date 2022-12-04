#! ./.venv/bin/python
# -*- coding: utf-8 -*-

# ---standard library---
import datetime
import logging
from logging import getLogger
from sre_constants import IN
import unicodedata

# ---third party library---
from pytchat import create

# ---local library---
from sql_connect import DatabaseConnect
import property


class ChatDataModule():
    def __init__(self):
        self.logger = getLogger(__name__)
        self.date = datetime.datetime.now().strftime('%Y-%m-%d-%H%M')
        self.table_name = 'chatdata'

    def get_chatdata(self, video_id:str):
        print(f'{video_id}')
        chat = create(video_id=video_id, force_replay=True)
        if chat.is_replay:
            self.logger.info('replay')
        with DatabaseConnect('livechat_database') as db:
            while chat.is_alive():
                try:
                    data = chat.get()
                    items = data.items
                    for c in items:
                        elapsedTimeStr = c.elapsedTime.replace(',','').split(':')
                        elapsedTime = 0
                        if (len(elapsedTimeStr) == 3): elapsedTime = int(elapsedTimeStr[0])*3600 + int(elapsedTimeStr[1])*60 + int(elapsedTimeStr[2])
                        elif (len(elapsedTimeStr) == 2): elapsedTime = int(elapsedTimeStr[0])*60 + int(elapsedTimeStr[1])
                        elif (len(elapsedTimeStr) == 1):
                            try: elapsedTime = int(elapsedTimeStr[0])
                            except:elapsedTime = 0
                        else:elapsedTime = 0

                        # self.logger.info(f"{c.datetime} | {c.elapsedTime} {elapsedTime} [{c.author.name}]- {c.message}")
                        author_name_width = 0
                        for name_c in c.author.name:
                            author_name_width += 1 if unicodedata.east_asian_width(name_c) in 'FWA' else 0
                        # print(f"{c.datetime.split()[0]:>10} {c.datetime.split()[1]:>8} | {c.elapsedTime:>8} {elapsedTime:>5} | [{c.author.name:^{24-max(0,author_name_width)}}] {c.message}")
                        sql = f'INSERT INTO {self.table_name} (\
                            id,\
                            videoId,\
                            name,\
                            channelId,\
                            chatType,\
                            timeStamp,\
                            datetime,\
                            message,\
                            amountValue,\
                            amountString,\
                            currency,\
                            isVerified,\
                            isOwner,\
                            isSponsor,\
                            isModerator\
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        value = (
                            c.id,
                            video_id,
                            c.author.name,
                            c.author.channelId,
                            c.type,
                            elapsedTime,
                            datetime.datetime.strptime(c.datetime, '%Y-%m-%d %H:%M:%S'),
                            c.message,
                            c.amountValue,
                            c.amountString,
                            c.currency,
                            c.author.isVerified,
                            c.author.isChatOwner,
                            c.author.isChatSponsor,
                            c.author.isChatModerator,
                        )
                        db.cursor.execute(sql, value)
                except KeyboardInterrupt:
                    chat.terminate()
                    break
                except Exception as e:
                    print(e)
                    chat.terminate()
                    break
        return

if __name__ == '__main__':
    import logging
    from logging import DEBUG, INFO, Logger, getLogger
    logging.basicConfig(
        level=INFO,
        format='[ %(levelname)-8s] %(asctime)s | %(name)-24s %(funcName)-16s| %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    fh = logging.FileHandler(filename='log/chat_data_search_system.log', encoding='utf-8')
    fh.setLevel=DEBUG
    fh.setFormatter(logging.Formatter('[ %(levelname)-8s] %(asctime)s | %(name)-32s %(funcName)-24s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))

    logger = getLogger(__name__)
    logger.addHandler(fh)


    cdm = ChatDataModule()
    cdm.get_chatdata('I4YMGy_0Pg4') #input('id:'))
    # score = cdm.count_score()

