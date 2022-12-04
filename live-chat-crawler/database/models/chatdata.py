from sqlalchemy import create_engine, Column, String, Integer, Unicode, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base

ModelBase  = declarative_base()

class ChatData(ModelBase):
    __tablename__ = 'chatdata'

    id = Column(String(256), primary_key=True)
    videoId = Column(String(256), primary_key=True)
    name = Column(Unicode(64))
    channelId = Column(String(200), nullable=False)
    chatType = Column(String(64))
    timeStamp = Column(Integer, nullable=False)
    datetime = Column(DateTime, nullable=False)
    message = Column(Unicode(512))
    amountValue = Column(Integer, nullable=False)
    amountString = Column(String(64))
    currency = Column(String(64))
    isVerified = Column(Boolean)
    isOwner = Column(Boolean)
    isSponsor = Column(Boolean)
    isModerator = Column(Boolean)