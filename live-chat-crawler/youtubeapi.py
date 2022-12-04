#! ./.venv/bin/python

# ---standard library---
from datetime import datetime as dt
from http.client import responses
import imp
import json
from time import sleep

# ---third party library---
import googleapiclient.errors
from googleapiclient.discovery import build
from dateutil.relativedelta import relativedelta

# ---local library---
import property

API_KEY = property.YOUTUBE_API_KEY
API_SERVICE_NAME = 'youtube'
API_VERSION = 'v3'

class YoutubeApi():

    def __init__(self):
        self.youtube = build(
            API_SERVICE_NAME,
            API_VERSION,
            developerKey = API_KEY
        )

    def get_video_list_all(self, channel_id, maxResults=50):
        video_id_list = []
        today = dt.today()
        limit = dt(2016, 1, 1)
        index = 0
        while today - relativedelta(months=index) > limit:
            request_count = 0
            search = self.youtube.search().list(
                    part = 'id',
                    channelId = channel_id,
                    type = 'video',
                    publishedAfter = (today - relativedelta(months=index+1)).isoformat()+'Z',
                    publishedBefore = (today - relativedelta(months=index)).isoformat()+'Z',
                    maxResults = maxResults
                )
            while True:
                try:
                    response = search.execute()
                except googleapiclient.errors.HttpError as e:
                    print(e.reason)

                with open('tmp/response.txt', mode='a', encoding='utf-8_sig') as f:
                    f.write(json.dumps(response, indent=2, ensure_ascii=False))

                for item in response.get('items', []):
                    video_id_list.append(item['id']['videoId'])

                if response['pageInfo']['resultsPerPage'] < 50:
                    break
                request_count += 1

                search = self.youtube.search().list_next(
                    search,
                    response
                )
                sleep(5)
            sleep(5)
            index += 1
        print(video_id_list)
        return video_id_list

    def get_video_list(self, channel_id, maxResults=50):
        search = self.youtube.search().list(
                part = 'id',
                channelId = channel_id,
                type = 'video',
                order='date',
                maxResults = maxResults
            )
        try:
            response = search.execute()
        except googleapiclient.errors.HttpError as e:
            raise(e)

        video_id_list = []
        for item in response.get('items', []):
            video_id_list.append(item['id']['videoId'])

        return video_id_list

    def get_channel_detail(self, channel_id):
        response = self.youtube.channels().list(
            part = 'id,snippet,brandingSettings,contentDetails,statistics,topicDetails',
            id = channel_id,
            # maxResults = '50'
        ).execute()
        return response

    def search_live(self, channel_id):
        response = self.youtube.search().list(
            part = 'id',
            channelId = channel_id,
            eventType = 'live',
            type = 'video'
        ).execute()
        return response

    def get_livedetail(self, video_id):
        response = self.youtube.videos().list(
            part = 'snippet,liveStreamingDetails',
            id = video_id
        ).execute()
        if 'liveStreamingDetails' not in response['items'][0].keys():
            raise
        return response

    def get_channel_name(self, response):
        name = response['items'][0]['snippet']['channelTitle']
        return name

    def get_title(self, response):
        title = response['items'][0]['snippet']['title']
        return title

    def get_thumbnail_url(self, response):
        url = response['items'][0]['snippet']['thumbnails']['maxres']['url']
        return url

    def get_islive(self, response):
        try:
            is_live = response['items'][0]['snippet']['liveBroadcastContent']
            if is_live == 'none':
                is_live = False
            elif is_live == 'live':
                is_live = True
            else:
                raise
        except:
            raise
        return is_live

    def get_starttime(self, response):
        try:
            actualStartTime = response['items'][0]['liveStreamingDetails']['actualStartTime']
        except:
            actualStartTime = '1970-01-01 00:00:00Z'
        return actualStartTime

    def get_endtime(self, response):
        try:
            actualEndTime = response['items'][0]['liveStreamingDetails']['actualEndTime']
        except:
            actualEndTime = '1970-01-01 00:00:00Z'
        return actualEndTime

    def get_starttime_UNIX(self, response):
        start_time = self.get_starttime(response=response)
        start_time = start_time.replace('T', ' ').replace('Z', '') + '+0000'
        start_time_UNIX = int(dt.strptime(start_time, '%Y-%m-%d %H:%M:%S%z').timestamp() * 1000)
        return start_time_UNIX

    def get_endtime_UNIX(self, response):
        end_time = self.get_endtime(response=response)
        end_time = end_time.replace('T', ' ').replace('Z', '') + '+0000'
        end_time_UNIX = int(dt.strptime(end_time, '%Y-%m-%d %H:%M:%S%z').timestamp() * 1000)
        return end_time_UNIX
    
    def get_Live(self, channel_id):
        response = self.youtube.liveBroadcasts().list(
            part = 'contentDetails',
            channelId = channel_id
        ).execute()
        return response


if __name__ == "__main__":
    api = YoutubeApi()
    api.get_video_list('test')
    # res = api.search_live(input())
    # res = api.get_livedetail(input('id:'))
    '''
    print(api.get_starttime_UNIX(res))
    print(api.get_endtime_UNIX(res))
    print(api.get_starttime(res))
    print(api.get_endtime(res))
    # print(type(res))
    # print(res)
    '''
    # print(res)
    # print(json.dumps(res, indent=2, ensure_ascii=False))
    # data = json.dumps(res, indent=2, ensure_ascii=False)
    # print(res)
    # with open('tmp/response.txt', mode='a', encoding='utf-8_sig') as f:
    #     f.write(res)


    # for item in res.get('items', []):
    #     print(json.dumps(item, indent=2, ensure_ascii=False))
    #     data = json.dumps(item, indent=2, ensure_ascii=False)
    #     with open('tmp/response.txt', mode='a', encoding='utf-8_sig') as f:
    #         f.write(data)


        # print(json.dumps(item['liveStreamingDetails'], indent=2, ensure_ascii=False))
        # print(item['snippet']['liveBroadcastContent'])
        # print(api.get_islive(res))
    # '''
    # actualStartTime = res['items'][0]['liveStreamingDetails']['actualStartTime']
    # print(res.get('liveStreamingDetails'))
    # print(res['liveStreamingDetails']['actualStartTime'])