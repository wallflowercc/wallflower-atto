#####################################################################################
#
#  Copyright (c) 2016 Eric Burger, Wallflower.cc
#
#  GNU Affero General Public License Version 3 (AGPLv3)
#
#  Should you enter into a separate license agreement after having received a copy of
#  this software, then the terms of such license agreement replace the terms below at
#  the time at which such license agreement becomes effective.
#
#  In case a separate license agreement ends, and such agreement ends without being
#  replaced by another separate license agreement, the license terms below apply
#  from the time at which said agreement ends.
#
#  LICENSE TERMS
#
#  This program is free software: you can redistribute it and/or modify it under the
#  terms of the GNU Affero General Public License, version 3, as published by the
#  Free Software Foundation. This program is distributed in the hope that it will be
#  useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
#  See the GNU Affero General Public License Version 3 for more details.
#
#  You should have received a copy of the GNU Affero General Public license along
#  with this program. If not, see <http://www.gnu.org/licenses/agpl-3.0.en.html>.
#
#####################################################################################

__version__ = '0.0.1'

import sqlite3
import json
import sys
import datetime
import copy
import re
import uuid
from base.wallflower_packet import WallflowerPacket
from base.wallflower_schema import getPythonType

from flask.ext.sqlalchemy import SQLAlchemy

db = SQLAlchemy()
        
class Network(db.Model):
    id = db.Column(db.Integer(), primary_key=True)
    network_id = db.Column(db.String(80), unique=True)
    network_details = db.Column(db.String(1000))
    created_at = db.Column(db.DateTime())
    updated_at = db.Column(db.DateTime())
    
    def __init__(self, network_id, network_details):
        self.network_id = network_id
        self.network_details = network_details
        self.created_at = datetime.datetime.utcnow()
        self.updated_at = self.created_at
        
    def __repr__(self):
        return '<Network %r>' % self.network_id
        
    def loadFromRow( self, row ):
        self.id = row[0]
        self.network_id = row[1]
        self.network_details = row[2]
        self.created_at = row[3]
        self.updated_at = row[4]
        
    def dict(self):
        return dict((col, getattr(self, col)) for col in self.__table__.columns.keys())
        
    def network_details_dict(self):
        return json.loads( self.network_details )
        
class Object(db.Model):
    id = db.Column(db.Integer(), primary_key=True)
    network_id = db.Column(db.String(80), unique=False)
    object_id = db.Column(db.String(80), unique=False)
    object_details = db.Column(db.String(1000))
    created_at = db.Column(db.DateTime())
    updated_at = db.Column(db.DateTime())
    
    def __init__(self, network_id, object_id, object_details):
        self.network_id = network_id
        self.object_id = object_id
        self.object_details = object_details
        self.created_at = datetime.datetime.utcnow()
        self.updated_at = self.created_at
        
    def __repr__(self):
        return '<Object %r>' % self.network_id+'.'+self.object_id

    def loadFromRow( self, row ):
        self.id = row[0]
        self.network_id = row[1]
        self.object_id = row[2]
        self.object_details = row[3]
        self.created_at = row[4]
        self.updated_at = row[5]

    def dict(self):
        return dict((col, getattr(self, col)) for col in self.__table__.columns.keys())
        
class Stream(db.Model):
    id = db.Column(db.Integer(), primary_key=True)
    network_id = db.Column(db.String(80), unique=False)
    object_id = db.Column(db.String(80), unique=False)
    stream_id = db.Column(db.String(80), unique=False)
    stream_details = db.Column(db.String(1000))
    points_details = db.Column(db.String(1000))
    points_current = db.Column(db.String(1000))
    created_at = db.Column(db.DateTime())
    updated_at = db.Column(db.DateTime())
    
    def __init__(self, network_id, object_id, stream_id, stream_details, points_details):
        self.network_id = network_id
        self.object_id = object_id
        self.stream_id = stream_id
        self.stream_details = stream_details
        self.points_details = points_details
        self.created_at = datetime.datetime.utcnow()
        self.updated_at = self.created_at
        
    def __repr__(self):
        return '<Stream %r>' % self.network_id+'.'+self.object_id+'.'+self.stream_id

    def loadFromRow( self, row ):
        self.id = row[0]
        self.network_id = row[1]
        self.object_id = row[2]
        self.stream_id = row[3]
        self.stream_details = row[4]
        self.points_details = row[5]
        self.points_current = row[6]
        self.created_at = row[7]
        self.updated_at = row[8]
        
    def dict(self):
        return dict((col, getattr(self, col)) for col in self.__table__.columns.keys())
        
        
def createPointsTable( table_name, data_type, data_length=0 ):
    metadata = db.MetaData()
    '''
    timestamp date
    users = Table('users', metadata,
         Column('timestamp', Integer, primary_key=True),
         Column('name', String),
         Column('fullname', String),
    )
    *(Column(wordCol, Unicode(255)) for wordCol in wordColumns))
    '''
    if 0 == data_length:
        if data_type is basestring:
            return db.Table(table_name, metadata,
                 db.Column('timestamp', db.DateTime(), primary_key=True),
                 db.Column('value', db.String(255))
            )
        elif data_type is int:
            return db.Table(table_name, metadata,
                 db.Column('timestamp', db.DateTime(), primary_key=True),
                 db.Column('value', db.Integer())
            )
        elif data_type is float:
            return db.Table(table_name, metadata,
                 db.Column('timestamp', db.DateTime(), primary_key=True),
                 db.Column('value', db.Float())
            )
        elif data_type is bool:
            return db.Table(table_name, metadata,
                 db.Column('timestamp', db.DateTime(), primary_key=True),
                 db.Column('value', db.Boolean())
            )
    else:
        if data_type is basestring:
            return db.Table(table_name, metadata,
                 db.Column('timestamp', db.DateTime(), primary_key=True),
                *(db.Column('value'+str(i), db.String(255)) for i in range(data_length))
            )
        elif data_type is int:
            return db.Table(table_name, metadata,
                 db.Column('timestamp', db.DateTime(), primary_key=True),
                *(db.Column('value'+str(i), db.Integer()) for i in range(data_length))
            )
        elif data_type is float:
            return db.Table(table_name, metadata,
                 db.Column('timestamp', db.DateTime(), primary_key=True),
                *(db.Column('value'+str(i), db.Float()) for i in range(data_length))
            )
        elif data_type is bool:
            return db.Table(table_name, metadata,
                 db.Column('timestamp', db.DateTime(), primary_key=True),
                *(db.Column('value'+str(i), db.Boolean()) for i in range(data_length))
            )
        