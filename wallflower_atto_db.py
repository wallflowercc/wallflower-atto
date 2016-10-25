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

import json
import sys
import datetime
import copy
import re
import uuid

from base.wallflower_packet import WallflowerPacket
from base.wallflower_schema import getPythonType

from wallflower_atto_models import Network, Object, Stream, createPointsTable

from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import select

class WallflowerDB:
    
    datetime_format_full = '%Y-%m-%dT%H:%M:%S.%fZ'
    
    print_debug = True
    # Internal db messages
    db_message = None
    
    # Response(s)
    # For read, response contains requested data
    # For create, update, delete, response contains the
    # validated request (which has been executed by the db)
    response = None
      
    db = None
    
    '''
    Print Messages
    '''
    def debug(self,text):
        if self.print_debug:
            print( text )
            sys.stdout.flush()
    
    def getCombinedResponse(self, request_packet ):
        def merge(a, b, path=None):
            "merges b into a"
            if path is None: path = []
            for key in b:
                if key in a:
                    if isinstance(a[key], dict) and isinstance(b[key], dict):
                        merge(a[key], b[key], path + [str(key)])
                    elif a[key] == b[key]:
                        pass # same leaf value
                    else:
                        raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
                else:
                    a[key] = b[key]
            return a
        return merge( self.db_message, request_packet.message_packet )
    
    '''
    Execute Network, Object, Stream, or Points Request
    '''
    def do(self,request,request_type,request_level,ids,at=None):
        if at is None:
            at = datetime.datetime.utcnow().isoformat() + 'Z'
        
        self.completed_request_tuple = ()
        self.db_message = {}
        
        request_packet = WallflowerPacket()
        request_packet.loadRequest(request,request_type,request_level)
        
        # Check if packet has request
        has_request, the_request = request_packet.hasRequest(request_level)
        self.debug('Has '+request_level+" "+request_type+" request: "+str(has_request))
        if not has_request:
            self.db_message.update({
                request_level+'-error': 'Invalid request',
                request_level+'-code': 400
            })
            self.db_message.update( request_packet.schema_packet  )
            self.debug( "Invalid request or schema error" )
            return self.db_message
        
        # Check if necessary elements/parents do or do not exist
        do_continue = self.doChecks(request_type,request_level,ids)
        if not do_continue:
            if request_level+'-code' not in self.db_message:
                self.db_message.update({
                    request_level+'-error': request_level.title()+' request could not be completed',
                    request_level+'-code': 400
                })
            return self.db_message
        
        # Finally, do request
        done = self.doRequest(the_request,request_type,request_level,ids,at)
        if not done:
            if request_level+'-code' not in self.db_message:
                self.db_message.update({
                    request_level+'-error': request_level.title()+' request could not be completed',
                    request_level+'-code': 400
                })
            #return self.db_message
        return self.db_message
        
    
    """
    Check if necessary elements do or do not exist
    """
    def doChecks(self, request_type, request_level, ids ):
        the_id = '.'.join(ids)
         
        if request_level == 'network':
            network_id = ids[0]
            # Try loading the network
            network_exists, net = self.networkExists((network_id,))
            if network_exists and request_type in ['create']:
                # Already exists
                self.db_message[request_level+'-message'] =\
                    request_level.title()+' '+the_id+' already exists. No changes made.'
                self.db_message[request_level+'-code'] = 304
                self.debug( self.db_message[request_level+'-message'] )
                return False
            elif not network_exists and request_type in ['read','update','delete','search']:
                # Does not exist.
                self.db_message[request_level+'-error'] =\
                    request_level.title()+' '+the_id+' does not exist and '+\
                    request_type+' request cannot be completed.'
                self.db_message[request_level+'-code'] = 404
                self.debug( self.db_message[request_level+'-error'] )
                return False
                
        elif request_level == 'object':
            network_id,object_id = ids
            # Try loading the network
            network_exists, net = self.networkExists((network_id,))
            if not network_exists:
                # Does not exist.
                self.db_message['network-error'] =\
                    'Network '+network_id+' does not exist and '+\
                    request_level+' '+request_type+' request cannot be completed.'
                self.db_message['network-code'] = 404
                self.debug( self.db_message['network-error'] )
                return False
            
            # Check for the object
            object_exists, obj = self.objectExists(ids)
            if object_exists and request_type in ['create']:
                # Already exists
                self.db_message[request_level+'-message'] =\
                    request_level.title()+' '+the_id+' already exists. No changes made.'
                self.db_message[request_level+'-code'] = 304
                self.debug( self.db_message[request_level+'-message'] )
                return False
            elif not object_exists and request_type in ['read','update','delete','search']:
                # Does not exist.
                self.db_message[request_level+'-error'] =\
                    request_level.title()+' '+the_id+' does not exist and '+\
                    request_type+' request cannot be completed.'
                self.db_message[request_level+'-code'] = 404
                self.debug( self.db_message[request_level+'-error'] )
                return False
            
        elif request_level == 'stream' or request_level == 'points':
            network_id,object_id,stream_id = ids
            # Try loading the network
            network_exists, net = self.networkExists((network_id,))
            if not network_exists:
                # Does not exist.
                self.db_message['network-error'] =\
                    'Network '+network_id+' does not exist and '+\
                    request_level+' '+request_type+' request cannot be completed.'
                self.db_message['network-code'] = 404
                self.debug( self.db_message['network-error'] )
                return False
            
            # Check for the object
            object_exists, obj = self.objectExists((network_id,object_id))
            if not object_exists:
                # Does not exist.
                self.db_message['object-error'] =\
                    'Object '+network_id+'.'+object_id+' does not exist and '+\
                    request_level+' '+request_type+' request cannot be completed.'
                self.db_message['object-code'] = 404
                self.debug( self.db_message['object-error'] )
                return False
            
            # Check for the stream
            stream_exists, stm = self.streamExists(ids)
            if stream_exists and request_type in ['create']:
                # Already exists
                self.db_message[request_level+'-message'] =\
                    request_level.title()+' '+the_id+' already exists. No changes made.'
                self.db_message[request_level+'-code'] = 304
                self.debug( self.db_message[request_level+'-message'] )
                return False
            elif not stream_exists and request_type in ['read','update','delete','search']:
                # Does not exist.
                self.db_message[request_level+'-error'] =\
                    request_level.title()+' '+the_id+' does not exist and '+\
                    request_type+' request cannot be completed.'
                self.db_message[request_level+'-code'] = 404
                self.debug( self.db_message[request_level+'-error'] )
                return False
        
        return True
        

    '''
    Route requests
    '''
    def doRequest(self,request,request_type,request_level,ids,at):
        if request_level == 'network':
            if request_type == 'create':
                return self.createNetwork(ids,request,at)
            elif request_type == 'read':
                return self.readNetwork(ids,request,at)
            elif request_type == 'update':
                return self.updateNetwork(ids,request,at)
            elif request_type == 'delete':
                return self.deleteNetwork(ids,request,at)
            elif request_type == 'search':
                return self.searchNetwork(ids,request,at)
        elif request_level == 'object':
            if request_type == 'create':
                return self.createObject(ids,request,at)
            elif request_type == 'read':
                return self.readObject(ids,request,at)
            elif request_type == 'update':
                return self.updateObject(ids,request,at)
            elif request_type == 'delete':
                return self.deleteObject(ids,request,at)
            elif request_type == 'search':
                return self.searchObject(ids,request,at)
        elif request_level == 'stream':
            if request_type == 'create':
                return self.createStream(ids,request,at)
            elif request_type == 'read':
                return self.readStream(ids,request,at)
            elif request_type == 'update':
                return self.updateStream(ids,request,at)
            elif request_type == 'delete':
                return self.deleteStream(ids,request,at)
            elif request_type == 'search':
                return self.searchStream(ids,request,at)
        elif request_level == 'points':
            if request_type == 'read':
                return self.readPoints(ids,request,at)
            elif request_type == 'update':
                return self.updatePoints(ids,request,at)
            elif request_type == 'delete':
                return self.deletePoints(ids,request,at)
            elif request_type == 'search':
                return self.searchPoints(ids,request,at)
                
        return False
                
    '''
    Route [__]Exists requests    
    '''
    def checkExists(self,request_level,ids):
        if request_level == 'network':
            return self.networkExists(ids)
        elif request_level == 'object':
            return self.objectExists(ids)
        elif request_level == 'stream':
            return self.streamExists(ids)
        elif request_level == 'points':
            return self.streamExists(ids)
            
        
    '''
    Create network. 
    Network must not already exist. 
    Assumes network info well formatted.
    '''
    def createNetwork(self,ids,create_network_request,at):
        network_id = ids[0]
        created = False
        
        try:
            # Automatically include...
            create_network_request['network-details']['created-at'] = at
            '''
            create_network_request['network-details']["network-master-key"] = uuid.uuid4().hex
            create_network_request['network-details']["network-keys"] = [
                {
                    "key": uuid.uuid4().hex,
                    "type": "read",
                    "name": "Read-Only Key",
                    "slug": "read-only-key",
                },
                {
                    "key": uuid.uuid4().hex,
                    "type": "create-read-update-delete",
                    "name": "Dashboard Key",
                    "slug": "dashboard-key",
                }
            ]
            '''
            network_details = json.dumps( create_network_request['network-details'] )
            create_network = Network(network_id, network_details)
            self.db.session.add(create_network)
            self.db.session.commit()
            
            created = True
            self.debug( "Network "+network_id+" Created" )
            self.db_message['network-message'] =\
                "Network "+network_id+" Created"
            self.db_message['network-code'] = 201
            self.db_message['network-details'] =\
                create_network_request['network-details']
            
        except OperationalError, err:
            self.db_message['network-error'] =\
                "Network "+network_id+" Not Created"
            self.db_message['network-code'] = 400
            self.debug( "Error: Network "+network_id+" Not Created" )
            self.debug( err )
            self.db.session.rollback()
            
        except:
            self.db_message['network-error'] =\
                "Network "+network_id+" Not Created"
            self.db_message['network-code'] = 400
            self.debug( "Error: Network "+network_id+" Not Created" )
            self.debug( "Unexpected error (0):"+str(sys.exc_info()) )
                        
        return created
    
    
    '''
    Create object. 
    Object must not already exist. Network must exist. 
    Assume object info well formatted.
    '''
    def createObject(self,ids,create_object_request,at):
        network_id,object_id = ids
        created = False
        
        try:
            # Automatically include...
            create_object_request['object-details']['created-at'] = at
            '''
            create_object_request['object-details']["object-master-key"] = uuid.uuid4().hex
            create_object_request['object-details']["object-keys"] = [
                {
                    "key": uuid.uuid4().hex,
                    "type": "read",
                    "name": "Read-Only Key",
                    "slug": "read-only-key",
                }
            ]
            '''
            object_details = json.dumps( create_object_request['object-details'] )
            create_object = Object(network_id, object_id, object_details)
            self.db.session.add(create_object)
            self.db.session.commit()
            
            created = True
            self.debug( "Object "+network_id+"."+object_id+" Created" )
            self.db_message['object-message'] =\
                "Object "+network_id+"."+object_id+" Created"
            self.db_message['object-code'] = 201
            self.db_message['object-details'] = \
                create_object_request['object-details']
            
        except OperationalError, err:
            self.db_message['object-error'] =\
                "Object "+network_id+"."+object_id+" Not Created"
            self.db_message['object-code'] = 400
            self.debug( "Error: Object "+network_id+"."+object_id+" Not Created" )
            self.debug( err )
            self.db.session.rollback()
            
        except:
            self.db_message['object-error'] =\
                "Object "+network_id+"."+object_id+" Not Created"
            self.db_message['object-code'] = 400
            self.debug( "Error: Object "+network_id+"."+object_id+" Not Created" )
            self.debug( "Unexpected error (1):"+str(sys.exc_info()) )
            
        return created
    
    
    '''
    Create stream and stream db. 
    Stream must not already exist. Network and Object must exist.
    Assume stream info well formated.
    '''
    def createStream(self,ids,create_stream_request,at):
        network_id,object_id,stream_id = ids
        created = False
        
        try:
            # Check ids
            assert len( re.findall( "[^a-zA-Z0-9\-\_]", network_id+object_id+stream_id ) ) == 0
            
            # Create Table
            # Note: To prevent SQL injection, ids
            # should have already been validated.
            table_name = network_id+'.'+object_id+'.'+stream_id
            
            points_details = create_stream_request['points-details']
            python_type = getPythonType( points_details['points-type']  )
            
            # Create SQLAlchemy table as needed
            points_table = createPointsTable( 
                table_name, 
                python_type, 
                points_details['points-length']
            )
            points_table.create(self.db.engine, checkfirst=True)
            self.db.session.commit()            
            
            create_stream_request['stream-details']['created-at'] = at
            '''
            create_stream_request['stream-details']["stream-master-key"] = uuid.uuid4().hex
            create_stream_request['stream-details']["stream-keys"] = [
                {
                    "key": uuid.uuid4().hex,
                    "type": "read",
                    "name": "Read-Only Key",
                    "slug": "read-only-key",
                }
            ]
            '''
            stream_details = json.dumps( create_stream_request['stream-details'] )
            points_details = json.dumps( create_stream_request['points-details'] )
            create_stream = Stream(network_id, object_id, stream_id, stream_details, points_details)
            self.db.session.add(create_stream)
            self.db.session.commit()

            created = True
            self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" Created" )
            self.db_message['stream-message'] =\
                "Stream "+network_id+"."+object_id+"."+stream_id+" Created"
            self.db_message['stream-code'] = 201
            self.db_message['stream-details'] =\
                create_stream_request['stream-details']
            self.db_message['points-details'] =\
                create_stream_request['points-details']
            
        except OperationalError, err:
            self.db_message['stream-error'] =\
                "Stream "+network_id+"."+object_id+"."+stream_id+" Not Created"
            self.db_message['stream-code'] = 400
            self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Created" )
            self.debug( err )
            self.db.session.rollback()
    
        except:
            # There was an error.
            self.db_message['stream-error'] =\
                "Stream "+network_id+"."+object_id+"."+stream_id+" Not Created"
            self.db_message['stream-code'] = 400
            self.debug( "Unexpected error (2):"+str(sys.exc_info()) )
            
        return created
    
    
    '''
    Read Network
    '''
    def readNetwork(self,ids,read_network_request,at):
        network_id = ids[0]
        read = False
        
        try:
            # Check for network
            net = Network.query.filter_by(network_id=network_id).one()
            if net is None:
                self.db_message['network-error'] = "Network "+network_id+" Not Read"
                self.db_message['network-code'] = 400
                self.debug( "Error: Network "+network_id+" Not Read" )
            else:
                self.db_message['network-details'] = json.loads( net.network_details )
                self.db_message['network-id'] = network_id
                
                # Check for objects
                self.db_message['objects'] = {}
                objects = Object.query.filter_by(network_id=network_id).all()
                if objects is not None:
                    for obj in objects:
                        object_id = obj.object_id
                        object_details = json.loads( obj.object_details )
                        self.db_message['objects'][object_id] = {
                            'object-id': object_id,
                            'object-details': object_details,
                            'streams': {}
                        }
                        
                # Check for streams
                streams = Stream.query.filter_by(
                    network_id=network_id).all()
                if streams is not None:
                    for stm in streams:
                        object_id = stm.object_id
                        stream_id = stm.stream_id
                        stream_details = json.loads( stm.stream_details )
                        points_details = json.loads( stm.points_details )
                        self.db_message['objects'][object_id]['streams'][stream_id] = {
                            'stream-id': stream_id,
                            'stream-details': stream_details,
                            'points-details': points_details,
                        }
                        
                        # Get points
                        table_name = network_id+'.'+object_id+'.'+stream_id
                        python_type = getPythonType( points_details['points-type']  )
                        
                        # Create SQLAlchemy table as needed
                        points_table = createPointsTable( 
                            table_name, 
                            python_type, 
                            points_details['points-length']
                        )
                        statement = select([points_table]).limit(5).order_by(points_table.c.timestamp.desc())
                        points_records = self.db.session.execute(statement).fetchall()
                        
                        points = []
                        for point in points_records:
                            if 0 == points_details['points-length']:
                                points.append({'at':point[0].isoformat() + 'Z','value':point[1]})
                            else:
                                points.append({'at':point[0].isoformat() + 'Z','value':point[1:]})
                        self.db_message['objects'][object_id]['streams'][stream_id]['points'] = points
                
                read = True
                self.db_message['network-message'] = "Network "+network_id+" Read"
                self.db_message['network-code'] = 200
                self.debug( "Network "+network_id+" Read" )
            
        except OperationalError, err:
            self.db_message['network-error'] = "Network "+network_id+" Not Read"
            self.db_message['network-code'] = 400
            self.debug( "Error: Network "+network_id+" Not Read" )
            self.debug( err )
            
        except:
            self.db_message['network-error'] = "Network "+network_id+" Not Read"
            self.db_message['network-code'] = 400
            self.debug( "Error: Network "+network_id+" Not Read" )
            self.debug( "Unexpected error (3):"+str(sys.exc_info()) )
            
        return read
    
    
    '''
    Read Object
    '''
    def readObject(self,ids,read_object_request,at):
        network_id,object_id = ids
        read = False
        
        try:
            # Check for object
            obj = Object.query.filter_by(
                network_id=network_id,
                object_id=object_id).one()
            if obj is None:
                self.db_message['object-error'] = "Object "+network_id+"."+object_id+" Not Read"
                self.db_message['object-code'] = 400
                self.debug( "Error: Object "+network_id+"."+object_id+" Not Read" )
            else:
                self.db_message['object-details'] = json.loads( obj.object_details )
                self.db_message['object-id'] = object_id
                
                # Check for streams
                self.db_message['streams'] = {}
                streams = Stream.query.filter_by(
                    network_id=network_id,
                    object_id=object_id).all()
                if streams is not None:
                    for stm in streams:
                        stream_id = stm.stream_id
                        stream_details = json.loads( stm.stream_details )
                        points_details = json.loads( stm.points_details )
                        self.db_message['streams'][stream_id] = {
                            'stream-id': stream_id,
                            'stream-details': stream_details,
                            'points-details': points_details
                        }
                        
                        # Get points
                        table_name = network_id+'.'+object_id+'.'+stream_id
                        python_type = getPythonType( points_details['points-type']  )
                        
                        # Create SQLAlchemy table as needed
                        points_table = createPointsTable( 
                            table_name, 
                            python_type, 
                            points_details['points-length']
                        )
                        statement = select([points_table]).limit(5).order_by(points_table.c.timestamp.desc())
                        contents = self.db.session.execute(statement).fetchall()
                        
                        points = []
                        for point in contents:
                            if 0 == points_details['points-length']:
                                points.append({'at':point[0].isoformat() + 'Z','value':point[1]})
                            else:
                                points.append({'at':point[0].isoformat() + 'Z','value':point[1:]})
                        self.db_message['streams'][stream_id]['points'] = points
                                
                self.db_message['object-message'] =\
                    "Object "+network_id+"."+object_id+" Read"
                self.db_message['object-code'] = 200
                self.debug( "Object "+network_id+"."+object_id+" Read" )
                read = True
            
        except OperationalError, err:
            self.db_message['object-error'] =\
                "Object "+network_id+"."+object_id+" Not Read"
            self.db_message['object-code'] = 400
            self.debug( "Error: Object "+network_id+"."+object_id+" Not Read" )
            self.debug( err )
            
        except:
            self.db_message['object-error'] =\
                "Object "+network_id+"."+object_id+" Not Read"
            self.db_message['object-code'] = 400
            self.debug( "Error: Object "+network_id+"."+object_id+" Not Read" )
            self.debug( "Unexpected error (4):"+str(sys.exc_info()) )            
            
        return read
                
    '''
    Read stream.
    '''
    def readStream(self,ids,read_stream_request,at):
        network_id,object_id,stream_id = ids
        read = False
        
        try:
            
            # Check for stream
            stm = Stream.query.filter_by(
                network_id=network_id,
                object_id=object_id,
                stream_id=stream_id).one()
            if stm is None:
                self.db_message['stream-error'] = \
                    "Stream "+network_id+"."+object_id+"."+stream_id+" Not Read"
                self.db_message['stream-code'] = 400
                self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Read" )
            else:
                stream_details = json.loads( stm.stream_details )
                points_details = json.loads( stm.points_details )
                self.db_message['stream-id'] = stream_id
                self.db_message['stream-details'] = stream_details
                self.db_message['points-details'] = points_details
                            
                # Get points
                table_name = network_id+'.'+object_id+'.'+stream_id
                python_type = getPythonType( points_details['points-type']  )
                
                # Create SQLAlchemy table as needed
                points_table = createPointsTable( 
                    table_name, 
                    python_type, 
                    points_details['points-length']
                )
                statement = select([points_table]).limit(5).order_by(points_table.c.timestamp.desc())
                contents = self.db.session.execute(statement).fetchall()
                
                points = []
                for point in contents:
                    if 0 == points_details['points-length']:
                        points.append({'at':point[0].isoformat() + 'Z','value':point[1]})
                    else:
                        points.append({'at':point[0].isoformat() + 'Z','value':point[1:]})
                self.db_message['points'] = points
                
                
                self.db_message['stream-message'] =\
                    "Stream "+network_id+"."+object_id+"."+stream_id+" Read"
                self.db_message['stream-code'] = 200
                self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" Read" )
                read = True
            
        except OperationalError, err:
            self.db_message['stream-error'] =\
                "Stream "+network_id+"."+object_id+"."+stream_id+" Not Read"
            self.db_message['stream-code'] = 400
            self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Read" )
            self.debug( err )
        except:
            self.db_message['stream-error'] =\
                "Stream "+network_id+"."+object_id+"."+stream_id+" Not Read"
            self.db_message['stream-code'] = 400
            self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Read" )
            self.debug( "Unexpected error (5):"+str(sys.exc_info()) )
            
        return read
                
    '''
    Read points from stream.
    '''
    def readPoints(self,ids,read_points_request,at):
        network_id,object_id,stream_id = ids
        read = False
        try:
            # Check for stream
            stm = Stream.query.filter_by(
                network_id=network_id,
                object_id=object_id,
                stream_id=stream_id).one()
            if stm is None:
                self.db_message['points-error'] = \
                    "Points "+network_id+"."+object_id+"."+stream_id+".points Not Read"
                self.db_message['points-code'] = 400
                self.debug( "Error: Points "+network_id+"."+object_id+"."+stream_id+".points Not Read" )
            else:
                points_details = json.loads( stm.points_details )
                self.db_message['stream-id'] = stream_id
                self.db_message['points-details'] = points_details
                
                # Get points
                table_name = network_id+'.'+object_id+'.'+stream_id
                python_type = getPythonType( points_details['points-type']  )
                
                # Create SQLAlchemy table as needed
                points_table = createPointsTable( 
                    table_name, 
                    python_type, 
                    points_details['points-length']
                )
                statement = select([points_table]).limit(100).order_by(points_table.c.timestamp.desc())
                contents = self.db.session.execute(statement).fetchall()
                
                points = []
                for point in contents:
                    if 0 == points_details['points-length']:
                        points.append({'at':point[0].isoformat() + 'Z','value':point[1]})
                    else:
                        points.append({'at':point[0].isoformat() + 'Z','value':point[1:]})
                self.db_message['points'] = points
                
                self.db_message['points-message'] =\
                    "Points "+network_id+"."+object_id+"."+stream_id+".points Read"
                self.db_message['points-code'] = 200
                self.debug( "Points "+network_id+"."+object_id+"."+stream_id+".points Read" )
                read = True
            
        except OperationalError, err:
            self.db_message['points-error'] =\
                "Points "+network_id+"."+object_id+"."+stream_id+".points Not Read"
            self.db_message['points-code'] = 400
            self.debug( "Error: Points "+network_id+"."+object_id+"."+stream_id+".points Not Read" )
            self.debug( err )
        except:
            self.db_message['points-error'] =\
                "Points "+network_id+"."+object_id+"."+stream_id+".points Not Read"
            self.db_message['points-code'] = 400
            self.debug( "Error: Points "+network_id+"."+object_id+"."+stream_id+".points Not Read" )
            self.debug( "Unexpected error (6):"+str(sys.exc_info()) )
            
        return read
    
            
    '''
    Update network. Assumes network info well formatted.
    '''
    def updateNetwork(self,ids,update_network_request,at):
        network_id = ids[0]
        updated = False
        
        try:
            # Check for network
            net = Network.query.filter_by(network_id=network_id).one()
            if net is None:
                self.db_message['network-error'] = "Network "+network_id+" Not Updated"
                self.db_message['network-code'] = 400
                self.debug( "Error: Network "+network_id+" Not Updated" )
            else:
                # Update network
                network_details = json.loads( net.network_details )
                update_network_request['network-details']['updated-at'] = at
                for key in update_network_request['network-details']:
                    network_details[key] = update_network_request['network-details'][key]
                net.network_details = json.dumps( network_details )
                #Network.update().where(network_id=network_id).values(network_details=net.network_details)
                net.updated_at = datetime.datetime.strptime(
                    at,
                    self.datetime_format_full
                )
                self.db.session.commit()
                
                updated = True
                self.db_message['network-message'] = "Network "+network_id+" Updated"
                self.db_message['network-code'] = 200
                self.db_message['network-id'] = network_id
                # Return only updated details
                self.db_message['network-details'] =\
                    update_network_request['network-details']
                self.debug( "Network "+network_id+" Updated" )
            
        except OperationalError, err:
            self.db_message['network-error'] = "Network "+network_id+" Not Updated"
            self.db_message['network-code'] = 400
            self.debug( "Error: Network "+network_id+" Not Updated" )
            self.debug( err )
            self.db.session.rollback()
            
        except:
            self.db_message['network-error'] = "Network "+network_id+" Not Updated"
            self.db_message['network-code'] = 400
            self.debug( "Error: Network "+network_id+" Not Updated" )
            self.debug( "Unexpected error (7):"+str(sys.exc_info()) )
        
        return updated

        
    '''
    Update object. Assumes object info well formatted.
    '''
    def updateObject(self,ids,update_object_request,at):
        network_id,object_id = ids
        updated = False

        try:
            # Check for object
            obj = Object.query.filter_by(
                network_id=network_id,
                object_id=object_id).one()
            if obj is None:
                self.db_message['object-error'] = "Object "+network_id+"."+object_id+" Not Updated"
                self.db_message['object-code'] = 400
                self.debug( "Error: Object "+network_id+"."+object_id+" Not Updated" )
            else:
                # Update object
                object_details = json.loads( obj.object_details )
                update_object_request['object-details']['updated-at'] = at
                for key in update_object_request['object-details']:
                    object_details[key] =\
                    update_object_request['object-details'][key]
                obj.object_details = json.dumps( object_details )
                #Object.update().where(network_id=network_id,object_id=object_id).values(object_details=obj.object_details)
                obj.updated_at = datetime.datetime.strptime(
                    at,
                    self.datetime_format_full
                )       
                self.db.session.commit()
                
                updated = True
                self.db_message['object-message'] =\
                    "Object "+network_id+"."+object_id+" Updated"
                self.db_message['object-code'] = 200
                self.db_message['object-id'] = object_id
                # Return only updated details
                self.db_message['object-details'] =\
                    update_object_request['object-details']
                self.debug( "Object "+network_id+"."+object_id+" Updated" )
            
        except OperationalError, err:
            self.db_message['object-error'] =\
                "Object "+network_id+"."+object_id+" Not Updated"
            self.db_message['object-code'] = 400
            self.debug( "Error: Object "+network_id+"."+object_id+" Not Updated" )
            self.debug( err )
            self.db.session.rollback()
            
        except:
            self.db_message['object-error'] =\
                "Object "+network_id+"."+object_id+" Not Updated"
            self.db_message['object-code'] = 400
            self.debug( "Error: Object "+network_id+"."+object_id+" Not Updated" )
            self.debug( "Unexpected error (8):"+str(sys.exc_info()) )

        return updated
        
    '''
    Update stream. Assumes points-details and points well formatted.
    '''
    def updateStream(self,ids,update_stream_request,at):
        network_id,object_id,stream_id = ids
        updated = False

        try:
            # Check for stream
            stm = Stream.query.filter_by(
                network_id=network_id,
                object_id=object_id,
                stream_id=stream_id).one()
            if stm is None:
                self.db_message['stream-error'] = \
                    "Stream "+network_id+"."+object_id+"."+stream_id+" Not Updated"
                self.db_message['stream-code'] = 400
                self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Updated" )
            else:
                # Update stream
                stream_details = json.loads( stm.stream_details )
                update_stream_request['stream-details']['updated-at'] = at
                for key in update_stream_request['stream-details']:
                    stream_details[key] = update_stream_request['stream-details'][key]
                stm.stream_details = json.dumps( stream_details )
                #Stream.update().where(network_id=network_id,object_id=object_id,stream_id=stream_id).values(stream_details=stm.stream_details)
                stm.updated_at = datetime.datetime.strptime(
                    at,
                    self.datetime_format_full
                )          
                self.db.session.commit()
                
                updated = True
                self.db_message['stream-message'] =\
                    "Stream "+network_id+"."+object_id+"."+stream_id+" Updated"
                self.db_message['stream-code'] = 200
                # Return only updated details
                self.db_message['stream-id'] = stream_id
                self.db_message['stream-details'] =\
                    update_stream_request['stream-details']            
                self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" Updated" )
                
        except OperationalError, err:
            self.db_message['stream-error'] =\
                "Stream "+network_id+"."+object_id+"."+stream_id+" Not Updated"
            self.db_message['stream-code'] = 400
            self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Updated" )
            self.debug( err )
            self.db.session.rollback()
            
        except:
            self.db_message['stream-error'] =\
                "Stream "+network_id+"."+object_id+"."+stream_id+" Not Updated"
            self.db_message['stream-code'] = 400
            self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Updated" )
            self.debug( "Unexpected error (9):"+str(sys.exc_info()) )
            
        return updated
        
    '''
    Update stream. Assumes points_details and points well formatted.
    '''
    def updatePoints(self,ids,update_points_request,at):
        network_id,object_id,stream_id = ids
        updated = False
        
        try:
            # Check for stream
            stm = Stream.query.filter_by(
                network_id=network_id,
                object_id=object_id,
                stream_id=stream_id).one()
            if stm is None:
                # TODO stream-error or points-error
                self.db_message['stream-error'] = \
                    "Stream "+network_id+"."+object_id+"."+stream_id+" Not Found"
                self.db_message['stream-code'] = 400
                self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Found" )
            else:
                
                points_details = json.loads( stm.points_details )
                points_details['updated-at'] = at
                python_type = getPythonType( points_details['points-type']  )

                # The new points                
                the_points_update = update_points_request['points']
                
                continue_update = True
                new_values = []
                new_index = []
                new_points = []
                for point in the_points_update:                 
                    # Update database table                
                    point_at = at
                    if 'at' in point:
                        point_at = point['at']
                    point_value = point['value']
                    
                    found_type = None
                    try:
                        found_type = type(point_value)
                        if points_details['points-length'] > 0:
                            assert isinstance(point_value,(list,tuple))
                            for i in range(len(point_value)):
                                found_type = type(point_value[i])
                                try:
                                    assert isinstance(point_value[i],python_type)
                                except:
                                    # TODO: Generate Warning
                                    if python_type is basestring:
                                        point_value[i] = basestring( point_value[i] )
                                    elif python_type == int:
                                        point_value[i] = int( point_value[i] )
                                    elif python_type == long:
                                        point_value[i] = long( point_value[i] )
                                    elif python_type == float:
                                        point_value[i] = float( point_value[i] )
                                    elif python_type == bool:
                                        point_value[i] = bool( point_value[i] )
                        else:
                            found_type = type(point_value)
                            try:
                                assert isinstance(point_value,python_type)
                            except:
                                # TODO: Generate Warning
                                if python_type is basestring:
                                    point_value = basestring( point_value )
                                elif python_type == int:
                                    point_value = int( point_value )
                                elif python_type == long:
                                    point_value = long( point_value )
                                elif python_type == float:
                                    point_value = float( point_value )
                                elif python_type == bool:
                                    point_value = bool( point_value )
                    except:
                        self.db_message['points-error'] =\
                            "Stream "+network_id+"."+object_id+"."+\
                            stream_id+" Point Value Not "+str(python_type)
                        self.db_message['points-code'] = 406
                        self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+ \
                            " Point Value Should Be "+str(python_type)+\
                            ", Not "+str(found_type) )
                        continue_update = False
                        break
                        
                    new_values.append( point_value )
                    new_index.append( point_at )
                    new_points.append({
                        'value': point_value,
                        'at': point_at
                    })
                
                # Check if point parsing was successful
                # Currently, does not support partial success
                if continue_update:
                    # Update points
                    table_name = network_id+'.'+object_id+'.'+stream_id
                    python_type = getPythonType( points_details['points-type']  )
                    
                    # Create SQLAlchemy table as needed
                    points_table = createPointsTable( 
                        table_name, 
                        python_type, 
                        points_details['points-length']
                    )
                    for i in range(len(new_points)):
                        if 0 == points_details['points-length']:
                            statement = points_table.insert().values(
                                timestamp = datetime.datetime.strptime(
                                    new_points[i]['at'],
                                    self.datetime_format_full
                                ),
                                value = new_points[i]['value']
                            )
                        else:
                            kwargs = {
                                'timestamp': datetime.datetime.strptime(
                                    new_points[i]['at'], 
                                    self.datetime_format_full
                                )
                            }
                            for j in range(points_details['points-length']):
                                kwargs['value'+str(j)] = new_points[i]['value'][j]
                            statement = points_table.insert().values(**kwargs)
                                
                         # TODO: Check Lists
                        self.db.session.execute(statement)
                    
                    # Set current value
                    new_points = sorted(new_points, key=lambda k: k['at'])
                    if stm.points_current is None:
                        stm.points_current = json.dumps( new_points[-1] )
                    else:
                        points_current = json.loads( stm.points_current )
                        if new_points[-1]['at'] > points_current['at']:
                            stm.points_current = json.dumps( new_points[-1] )
                    
                    # Update min and max
                    if len(new_points) > 0 and python_type in (int,long,float):
                        min_val = new_points[0]['value']
                        max_val = new_points[0]['value']
                        if all(k in points_details for k in ("min-value","max-value")):
                            min_val = points_details['min-value']
                            max_val = points_details['max-value']
                            
                        for i in range(len(new_points)):
                            if new_points[i]['value'] > max_val:
                                max_val = new_points[i]['value']
                            elif new_points[i]['value'] < min_val:
                                min_val = new_points[i]['value']
                        
                        points_details['min-value'] = min_val
                        points_details['max-value'] = max_val
                    
                        stm.points_details = json.dumps( points_details )
                    
                    # Commit Changes
                    stm.updated_at = datetime.datetime.strptime(
                        at,
                        self.datetime_format_full
                    )
                    self.db.session.commit()
                    
                    updated = True
                    
                    self.db_message['points-message'] =\
                        "Points "+network_id+"."+object_id+"."+stream_id+".points Updated"
                    self.db_message['points-code'] = 200
                    # Return only updated details
                    self.db_message['points'] = the_points_update
                    self.debug( "Points "+network_id+"."+object_id+"."+stream_id+".points Updated" )
            
        except OperationalError, err:
            self.db_message['points-error'] =\
                "Points "+network_id+"."+object_id+"."+stream_id+".points Not Updated"
            self.db_message['points-code'] = 400
            self.debug( "Points "+network_id+"."+object_id+"."+stream_id+".points Not Updated" )
            self.debug( err )
            self.db.session.rollback()
            
        except:
            self.db_message['points-error'] =\
                "Points "+network_id+"."+object_id+"."+stream_id+".points Not Updated"
            self.db_message['points-code'] = 400
            self.debug( "Points "+network_id+"."+object_id+"."+stream_id+".points Not Updated" )
            self.debug( "Unexpected error (10):"+str(sys.exc_info()) )
            
        return updated


    '''
    Delete network. 
    TODO: It is unnecessary to check for network before deleting
    '''
    def deleteNetwork(self,ids,delete_network_request,at,update_message=True):
        network_id = ids[0]
        deleted = False        

        try:
            # Check for network
            net = Network.query.filter_by(network_id=network_id).one()
            if net is None:
                if update_message:
                    self.db_message['network-error'] = "Network "+network_id+" Not Deleted"
                    self.db_message['network-code'] = 400
                    self.debug( "Error: Network "+network_id+" Not Deleted" )
            else:
                # Delete all objects
                objects = Object.query.filter_by(
                    network_id=network_id).all()
                for obj in objects:
                    self.deleteObject((network_id,obj.object_id),None,at,False)                                                 

                # Delete network
                self.db.session.delete(net)
                self.db.session.commit()
                
                deleted = True
                if update_message:
                    self.db_message['network-message'] =\
                        "Network "+network_id+" Deleted"
                    self.db_message['network-code'] = 200
                self.debug( "Network "+network_id+" Deleted" )

        except OperationalError, err:
            if update_message:
                self.db_message['network-error'] =\
                    "Network "+network_id+" Not Deleted"
                self.db_message['network-code'] = 400
            self.debug( "Error: Network "+network_id+" Not Deleted" )
            self.debug( err )
            self.db.session.rollback()
    
        except:
            # There was an error.
            if update_message:
                self.db_message['network-error'] =\
                    "Network "+network_id+" Not Deleted"
                self.db_message['network-code'] = 400
            self.debug( "Unexpected error (11):"+str(sys.exc_info()) )
            
        return deleted

    '''
    Delete object. 
    TODO: It is unnecessary to check for object before deleting
    '''
    def deleteObject(self,ids,delete_object_request,at,update_message=True):
        network_id,object_id = ids
        deleted = False
        
        try:
            # Check for object
            obj = Object.query.filter_by(
                network_id=network_id,
                object_id=object_id).one()
            if obj is None:
                if update_message:
                    self.db_message['object-error'] = "Object "+network_id+"."+object_id+" Not Deleted"
                    self.db_message['object-code'] = 400
                    self.debug( "Error: Object "+network_id+"."+object_id+" Not Deleted" )
                
            else:
                # Delete all streams
                streams = Stream.query.filter_by(
                    network_id=network_id,
                    object_id=object_id).all()
                for stm in streams:
                    self.deleteStream((network_id,object_id,stm.stream_id),None,at,False)
                
                # Delete object
                self.db.session.delete(obj)
                self.db.session.commit()
                
                deleted = True
                if update_message:
                    self.db_message['object-message'] =\
                        "Object "+network_id+"."+object_id+" Deleted"
                    self.db_message['object-code'] = 200
                self.debug( "Object "+network_id+"."+object_id+" Deleted" )
    
        except OperationalError, err:
            if update_message:
                self.db_message['object-error'] =\
                    "Object "+network_id+"."+object_id+" Not Deleted"
                self.db_message['object-code'] = 400
            self.debug( "Error: Object "+network_id+"."+object_id+" Not Deleted" )
            self.debug( err )
            self.db.session.rollback()
    
        except:
            # There was an error.
            if update_message:
                self.db_message['object-error'] =\
                    "Object "+network_id+"."+object_id+" Not Deleted"
                self.db_message['object-code'] = 400
            self.debug( "Unexpected error (12):"+str(sys.exc_info()) )
            
        return deleted
        
    '''
    Delete stream. 
    TODO: It is unnecessary to check for stream before deleting
    '''
    def deleteStream(self,ids,delete_stream_request,at,update_message=True):
        network_id,object_id,stream_id = ids
        deleted = False
        
        try:
            # Check for stream
            stm = Stream.query.filter_by(
                network_id=network_id,
                object_id=object_id,
                stream_id=stream_id).one()
            if stm is None:
                if update_message:
                    self.db_message['stream-error'] = \
                        "Stream "+network_id+"."+object_id+"."+stream_id+" Not Deleted"
                    self.db_message['stream-code'] = 400
                    self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Deleted" )
            else:  
                # Drop table
                table_name = network_id+'.'+object_id+'.'+stream_id
                # Actual type and length not needed to drop/delete table
                python_type = int
                points_length = 0
                
                # Create SQLAlchemy table as needed
                points_table = createPointsTable( 
                    table_name, 
                    python_type, 
                    points_length
                )
                points_table.drop(self.db.engine, checkfirst=True)
                self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" DB Deleted" )
                
                # Delete stream
                self.db.session.delete(stm)
                self.db.session.commit()
                
                deleted = True
                if update_message:
                    self.db_message['stream-message'] =\
                        "Stream "+network_id+"."+object_id+"."+stream_id+" Deleted"
                    self.db_message['stream-code'] = 200
                self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" Deleted" )
                    
        except OperationalError, err:
            if update_message:
                self.db_message['stream-error'] =\
                    "Stream "+network_id+"."+object_id+"."+stream_id+" Not Deleted"
                self.db_message['stream-code'] = 400
            self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Deleted" )
            self.debug( err )
            self.db.session.rollback()
    
        except:
            # There was an error.
            if update_message:
                self.db_message['stream-error'] =\
                    "Stream "+network_id+"."+object_id+"."+stream_id+" Not Deleted"
                self.db_message['stream-code'] = 400
            self.debug( "Unexpected error (13):"+str(sys.exc_info()) )
        
        return deleted

    '''
    Delete points
    '''
    def deletePoints(self,ids,delete_points_request,at):
        network_id,object_id,stream_id = ids
        deleted = False
        
        try:
            # Delete points from table
            table_name = network_id+'.'+object_id+'.'+stream_id
            # Actual type and length not needed to delete points from table
            python_type = int
            points_length = 0
            
            # Create SQLAlchemy table as needed
            points_table = createPointsTable( 
                table_name, 
                python_type, 
                points_length
            )
            
            # Start delete statement
            statement = points_table.delete()
            
            # Expand statement according to details
            delete_points_details = delete_points_request['points']
            if 'before' in delete_points_details:
                before = datetime.datetime.strptime( 
                    delete_points_details['before'], 
                    self.datetime_format_full
                )
                statement = statement.where( points_table.c.timestamp < before )
            if 'after' in delete_points_details:
                after = datetime.datetime.strptime( 
                    delete_points_details['after'], 
                    self.datetime_format_full
                )
                statement = statement.where( points_table.c.timestamp > after )
            if 'except' in delete_points_details:
                # Select the most recent N points and find the
                # timestamp of the oldest point. Delete points
                # that come before this point.
                except_statement = select([points_table]).\
                    limit(delete_points_details['except']).\
                    order_by(points_table.c.timestamp.desc())
                contents = self.db.session.execute(except_statement).fetchall()
                except_after = contents[-1][0]
                statement = statement.where( points_table.c.timestamp < except_after )
            
            self.db.session.execute(statement)
            self.db.session.commit()
            
            deleted = True
            
            self.db_message['points-message'] =\
                "Points "+network_id+"."+object_id+"."+stream_id+".points Deleted"
            self.db_message['points-code'] = 200
            self.debug( "Points "+network_id+"."+object_id+"."+stream_id+".points Deleted" )
                    
        except OperationalError, err:
            self.db_message['points-error'] =\
                "Points "+network_id+"."+object_id+"."+stream_id+".points Not Deleted"
            self.db_message['points-code'] = 400
            self.debug( "Error: Points "+network_id+"."+object_id+"."+stream_id+".points Not Deleted" )
            self.debug( err )
            self.db.session.rollback()
    
        except:
            # There was an error.
            self.db_message['points-error'] =\
                "Points "+network_id+"."+object_id+"."+stream_id+".points Not Deleted"
            self.db_message['points-code'] = 400
            self.debug( "Unexpected error (13):"+str(sys.exc_info()) )
        
        return deleted
        
        
    """
    '''
    Search Network
    '''
    def searchNetwork(self,ids,search_network_request,at):
        network_id = ids[0]
        # TODO:
        try:
            # Assert network exists
            assert ('network-id' in self.networks[network_id])
            self.db_message = copy.deepcopy( self.networks[network_id] )
            self.db_message['network-message'] =\
                "Network "+network_id+" Searched"
            self.db_message['network-code'] = 200
            self.debug( "Network "+network_id+" Searched" )
            return True
        except:
            self.db_message['network-error'] =\
                "Network "+network_id+" Not Searched"
            self.db_message['network-code'] = 400
            self.debug( "Error: Network "+network_id+" Not Searched" )
            self.debug( "Unexpected error (14):"+str(sys.exc_info()) )
        return False

        
    '''
    Search Object
    '''
    def searchObject(self,ids,search_object_request,at):
        network_id,object_id = ids
        # TODO:
        try:
            # Assert object exists
            assert (object_id in self.networks[network_id]['objects'])
            self.db_message =\
                copy.deepcopy( self.networks[network_id]['objects'][object_id] )
            self.db_message['object-message'] =\
                "Object "+network_id+"."+object_id+" Searched"
            self.db_message['object-code'] = 200
            self.debug( "Object "+network_id+"."+object_id+" Searched" )
            return True
        except:
            self.db_message['object-error'] =\
                "Object "+network_id+"."+object_id+" Not Searched"
            self.db_message['object-code'] = 400
            self.debug( "Error: Object "+network_id+"."+object_id+" Not Searched" )
            self.debug( "Unexpected error (15):"+str(sys.exc_info()) )
        return False
                
    '''
    Search stream.
    '''
    def searchStream(self,ids,search_stream_request,at):
        network_id,object_id,stream_id = ids
        # TODO:
        try:
            # Assert stream exists
            assert (stream_id in self.networks[network_id]['objects'][object_id]['streams'])
            self.db_message =\
                copy.deepcopy( self.networks[network_id]['objects'][object_id]['streams'][stream_id] )
            self.db_message['stream-message'] =\
                "Stream "+network_id+"."+object_id+"."+stream_id+" Searched"
            self.db_message['stream-code'] = 200
            self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" Searched" )
            return True
        except:
            self.db_message['stream-error'] =\
                "Stream "+network_id+"."+object_id+"."+stream_id+" Not Searched"
            self.db_message['stream-code'] = 400
            self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Searched" )
            self.debug( "Unexpected error (16):"+str(sys.exc_info()) )
        return False
     """
     
    '''
    Search points from stream.
    '''
    def searchPoints(self,ids,search_points_request,at):
        network_id,object_id,stream_id = ids
        searched = False
        
        try:
            # Check for stream
            stm = Stream.query.filter_by(
                network_id=network_id,
                object_id=object_id,
                stream_id=stream_id).one()
            if stm is None:
                # TODO stream-error or points-error
                self.db_message['stream-error'] = \
                    "Stream "+network_id+"."+object_id+"."+stream_id+" Not Found"
                self.db_message['stream-code'] = 400
                self.debug( "Error: Stream "+network_id+"."+object_id+"."+stream_id+" Not Found" )
            else:
                
                points_details = json.loads( stm.points_details )
                
                # Search for points in table
                table_name = network_id+'.'+object_id+'.'+stream_id
                python_type = getPythonType( points_details['points-type']  )
                points_length = points_details['points-length'] 
                
                # Create SQLAlchemy table as needed
                points_table = createPointsTable( 
                    table_name, 
                    python_type, 
                    points_length
                )
                
                # Start search statement
                statement = select([points_table]).order_by(points_table.c.timestamp.desc())
                
                # Expand statement according to details
                search_points_details = search_points_request['points']
                if 'start' in search_points_details:
                    start = datetime.datetime.strptime( 
                        search_points_details['start'], 
                        self.datetime_format_full
                    )
                    statement = statement.where( points_table.c.timestamp >= start )
                if 'end' in search_points_details:
                    end = datetime.datetime.strptime( 
                        search_points_details['end'], 
                        self.datetime_format_full
                    )
                    statement = statement.where( points_table.c.timestamp <= end )
                    
                limit = 100
                if 'limit' in search_points_details:
                    if search_points_details['limit'] < 1000:
                        limit = search_points_details['limit'] 
                    else:
                        limit = 1000
                statement = statement.limit(limit)
                
                contents = self.db.session.execute(statement).fetchall()
                
                points = []
                for point in contents:
                    if 0 == points_details['points-length']:
                        points.append({'at':point[0].strftime(self.datetime_format_full),'value':point[1]})
                    else:
                        points.append({'at':point[0].strftime(self.datetime_format_full),'value':point[1:]})
                
                self.db_message['points-details'] = points_details
                if len(points) > 1 and isinstance(points[0]['value'],(int,long,float)):
                    min_val = points[0]['value']
                    max_val = points[0]['value']
                    for i in range(1,len(points)):
                        if points[i]['value'] > max_val:
                            max_val = points[i]['value']
                        elif points[i]['value'] < min_val:
                            min_val = points[i]['value']
                    self.db_message['points-details']['search-min-value'] = min_val
                    self.db_message['points-details']['search-max-value'] = max_val
                
                searched = True
                self.db_message['points'] = points
                self.db_message['points-message'] =\
                    "Points "+network_id+"."+object_id+"."+stream_id+".points Searched"
                self.db_message['points-code'] = 200
                self.debug( "Points "+network_id+"."+object_id+"."+stream_id+".points Searched" )
                        
        except OperationalError, err:
            self.db_message['points-error'] =\
                "Points "+network_id+"."+object_id+"."+stream_id+".points Not Searched"
            self.db_message['points-code'] = 400
            self.debug( "Error: Points "+network_id+"."+object_id+"."+stream_id+".points Not Searched" )
            self.debug( err )
            self.db.session.rollback()
    
        except:
            # There was an error.
            self.db_message['points-error'] =\
                "Points "+network_id+"."+object_id+"."+stream_id+".points Not Searched"
            self.db_message['points-code'] = 400
            self.debug( "Unexpected error (14):"+str(sys.exc_info()) )
        
        return searched
        
    
    
    '''
    Check if network exists.    
    '''
    def networkExists(self,ids):
        network_id = ids[0]
        network_record = Network.query.filter_by(network_id=network_id).first()
        if network_record is None:
            self.debug( "Network "+network_id+" Not Found" )
            return False, None
        else:
            self.debug( "Network "+network_id+" Found" )
            return True, network_record
        
    '''
    Check if object exists.
    '''
    def objectExists(self,ids):
        network_id,object_id = ids
        object_record = Object.query.filter_by(network_id=network_id,object_id=object_id).first()
        if object_record is None:
            self.debug( "Object "+network_id+"."+object_id+" Not Found" )
            return False, None
        else:
            self.debug( "Object "+network_id+"."+object_id+" Found" )
            return True, object_record
        
    '''
    Check if stream exists.
    '''
    def streamExists(self,ids):
        network_id,object_id,stream_id = ids
        stream_record = Stream.query.filter_by(network_id=network_id,object_id=object_id,stream_id=stream_id).first()
        if stream_record is None:
            self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" Not Found" )
            return False, None
        else:
            self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" Found" )
            return True, stream_record
            '''
            try:
                contents = self.db.session.execute(select([points_table]).limit(1)).fetchall()

            except:
                self.debug( "Stream "+network_id+"."+object_id+"."+stream_id+" DB Not Found" )
                self.debug( err )
                return False, None
            '''