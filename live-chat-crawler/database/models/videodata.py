from sqlalchemy import create_engine, Column, String, Integer, Unicode, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base

ModelBase  = declarative_base()

class VideoData(ModelBase):
    __tablename__ = 'videodata'

    id = Column(String(256), primary_key=True)
    channelId = Column(String(256), nullable=False)
    title = Column(Unicode(256))
    description = Column(Unicode(5192))
    datetime = Column(DateTime, nullable=False)
    isLive = Column(Boolean)
    hasLiveChat = Column(Boolean)