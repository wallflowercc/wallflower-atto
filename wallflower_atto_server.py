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

from flask import Flask, request, jsonify, make_response, send_from_directory, render_template
from wallflower_atto_models import db
from wallflower_atto_db import WallflowerDB

#import re
import datetime

# Load config
config = {
    'network-id': 'local',
    'enable_ws': False,
    'http_port': 5000,
    'ws_port': 5050,
    'database': {
        'name': 'wallflower_db',
        'type': 'sqlite'
    }
}

try:
    with open('wallflower_config.json', 'rb') as f:
        wallflower_config = json.load(f)
        config.update( wallflower_config )
except:
    print( "Invalid wallflower_config.json file" )

app = Flask(__name__)

if config['database']['type'] == 'sqlite':
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///'+config['database']['name']+'.sqlite'    
elif config['database']['type'] == 'postgresql':
    app.config['SQLALCHEMY_DATABASE_URI'] = 'postgres://'+config['database']['user']+':'+config['database']['password']+'@'+config['database']['host']+':'+str(config['database']['port'])+'/'+config['database']['database']
elif config['database']['type'] == 'postgresql-heroku':
    import os
    app.config['SQLALCHEMY_DATABASE_URI'] = os.environ["DATABASE_URL"]
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Create database connection object
db.init_app(app)
atto_db = WallflowerDB()
atto_db.db = db

# Initialize db with Flask app context   
# Note: current_app points to app               
with app.app_context():
    # Create database and tables
    #db.drop_all() 
    db.create_all()

# Routes
# Route index/dashboard html file
@app.route('/', methods=['GET'])
def root():
    # Return WebSocket or non-WebSocket Interface
    data = {}
    data['enable_ws'] = False
    if config['enable_ws']:
        data['enable_ws'] = True
    return render_template('atto/index.html', data=data)

# Route static font files
@app.route('/fonts/<path:filename>')
def send_font_file(filename):
    filename = 'fonts/'+filename
    response = make_response(send_from_directory('static', filename))
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response
    
# Route static files
@app.route('/<path:filename>')
def send_file(filename):
    return send_from_directory('static', filename)

# Route Network Requests
@app.route('/n/<network_id>', methods=['GET'])
@app.route('/networks/<network_id>', methods=['GET'])
def networks(network_id):
    response_type = request.args.get('response-type','json',type=str)
    response_type = request.args.get('rt',response_type,type=str)
    
    # Check network id
    if network_id != config['network-id']:
        response = {
            'network-error': "The Wallflower-Atto server only allows for one network with the id "+config['network-id'],
            'network-code': 400
        }
        if response_type == 'csv':
            response = make_response( 'nc,400' )
            response.headers["Content-type"] = "text/csv"
            return response
        else:
            return jsonify(**response)
        
    at = datetime.datetime.utcnow().isoformat() + 'Z'
    
    response = {
        'network-id': config['network-id']
    }
    
    if request.method == 'GET':
        # Read Network Details
        network_request = {
            'network-id': config['network-id']
        }
        
        atto_db.do(network_request,'read','network',(config['network-id'],),at)
        response.update( atto_db.db_message )
        
    if response_type == 'csv':
        response = make_response( 'nc,'+str(response['network-code']) )
        response.headers["Content-type"] = "text/csv"
        return response
    else:
        return jsonify(**response)

# Route Object Requests
@app.route('/n/'+config['network-id']+'/o/<object_id>', methods=['GET','PUT','POST','DELETE'])
@app.route('/networks/'+config['network-id']+'/objects/<object_id>', methods=['GET','PUT','POST','DELETE'])
def objects(object_id):
    response_type = request.args.get('response-type','json',type=str)
    response_type = request.args.get('rt',response_type,type=str)
    
    at = datetime.datetime.utcnow().isoformat() + 'Z'

    object_request = {
        'object-id': object_id
    }
    
    response = {
        'network-id': config['network-id'],
        'object-id': object_id
    }
    
    if request.method == 'GET': # Read
        # Read Object Details
        atto_db.do(object_request,'read','object',(config['network-id'],object_id),at)
        response.update( atto_db.db_message )
        
    elif request.method == 'PUT': # Create
        # Create Object
        object_request['object-details'] = {
            'object-name': object_id
        }
        object_name = request.args.get('object-name',None,type=str)
        if object_name is not None:
            object_request['object-details']['object-name'] = object_name

        atto_db.do(object_request,'create','object',(config['network-id'],object_id),at)
        response.update( atto_db.db_message )
        
    elif request.method == 'POST': 
        # Update Object Details
        object_request['object-details'] = {
            'object-name': object_id
        }
        object_name = request.args.get('object-name',None,type=str)
        if object_name is not None:
            object_request['object-details']['object-name'] = object_name
            
        atto_db.do(object_request,'update','object',(config['network-id'],object_id),at)
        response.update( atto_db.db_message )
        
    elif request.method == 'DELETE': 
        # Delete Object
        atto_db.do(object_request,'delete','object',(config['network-id'],object_id),at)
        response.update( atto_db.db_message )
        
    if response_type == 'csv':
        response = make_response( 'oc,'+str(response['object-code']) )
        response.headers["Content-type"] = "text/csv"
        return response
    else:
        return jsonify(**response)


# Route Object Requests
@app.route('/n/'+config['network-id']+'/o/<object_id>/s/<stream_id>', methods=['GET','PUT','POST','DELETE'])
@app.route('/networks/'+config['network-id']+'/objects/<object_id>/streams/<stream_id>', methods=['GET','PUT','POST','DELETE'])
def streams(object_id,stream_id):
    response_type = request.args.get('response-type','json',type=str)
    response_type = request.args.get('rt',response_type,type=str)
    
    at = datetime.datetime.utcnow().isoformat() + 'Z'
    
    stream_request = {
        'stream-id': stream_id
    }
    
    response = {
        'network-id': config['network-id'],
        'object-id': object_id,
        'stream-id': stream_id
    }
    
    if request.method == 'GET': # Read
        # Read Object Details
        atto_db.do(stream_request,'read','stream',(config['network-id'],object_id,stream_id),at)
        response.update( atto_db.db_message )
        
    elif request.method == 'PUT': # Create
        # Create Stream
        stream_request['stream-details'] = {
            'stream-name': object_id,
            'stream-type': 'data'
        }
        stream_request['points-details'] = {
            'points-type': 'i',
            'points-length': 0
        }
        stream_name = request.args.get('stream-name',None,type=str)
        if stream_name is not None:
            stream_request['stream-details']['stream-name'] = stream_name
            
        points_type = request.args.get('points-type',None,type=str)
        if stream_name is not None and points_type in ['i','f','s']:
            stream_request['points-details']['points-type'] = points_type
        
        atto_db.do(stream_request,'create','stream',(config['network-id'],object_id,stream_id),at)
        response.update( atto_db.db_message )
        
    elif request.method == 'POST': 
        # Update Object Details
        stream_request['stream-details'] = {
            'object-name': object_id
        }
        stream_name = request.args.get('stream-name',None,type=str)
        if stream_name is not None:
            stream_request['stream-details']['stream-name'] = stream_name

        atto_db.do(stream_request,'update','stream',(config['network-id'],object_id,stream_id),at)
        response.update( atto_db.db_message )
        
    elif request.method == 'DELETE': 
        # Delete Object
        atto_db.do(stream_request,'delete','stream',(config['network-id'],object_id,stream_id),at)
        response.update( atto_db.db_message )
        
    if response_type == 'csv':
        response = make_response( 'sc,'+str(response['stream-code']) )
        response.headers["Content-type"] = "text/csv"
        return response
    else:
        return jsonify(**response)
    



# Route Stream Requests
@app.route('/n/'+config['network-id']+'/o/<object_id>/s/<stream_id>/p', methods=['GET','POST','DELETE'])
@app.route('/networks/'+config['network-id']+'/objects/<object_id>/streams/<stream_id>/points', methods=['GET','POST','DELETE'])
def points(object_id,stream_id):
    response_type = request.args.get('response-type','json',type=str)
    response_type = request.args.get('rt',response_type,type=str)
    
    at = datetime.datetime.utcnow().isoformat() + 'Z'
    
    points_request = {
        'stream-id': stream_id,
        'points': []
    }

    response = {
        'network-id': config['network-id'],
        'object-id': object_id,
        'stream-id': stream_id
    }
    
    if request.method == 'GET':
        # Read Points (Use Search Instead Of Read)
        # Max number of data points (Optional)
        limit = request.args.get('points-limit',None,type=int)
        # Start date/time (Optional)
        start = request.args.get('points-start',None,type=str)
        # End date/time (Optional)
        end = request.args.get('points-end',None,type=str)
        
        # Points Search Input
        point_search = {}
        if limit is not None and isinstance(limit,int):
            point_search['limit'] = limit
        if start is not None and isinstance(start,str):
            point_search['start'] = start
        if end is not None and isinstance(end,str):
            point_search['end'] = end
        
        points_request['points'] = point_search
        
        atto_db.do(points_request,'search','points',(config['network-id'],object_id,stream_id),at)
        response.update( atto_db.db_message )
        
    elif request.method == 'POST':
        # Update Points
        # Point value (Required)
        point_value = request.args.get('points-value',None)
        if point_value is None:
            response['points-code'] = 406
            response['points-message'] = 'No value received'
            if response_type == 'csv':
                response = make_response( 'pc,406' )
                response.headers["Content-type"] = "text/csv"
                return response
            else:
                return jsonify(**response)
            
        # At date/time (Optional)
        point_at = request.args.get('points-at',at,type=str)
        try:
            datetime.datetime.strptime(point_at, "%Y-%m-%dT%H:%M:%S.%fZ")
        except:
            response['points-code'] = 400
            response['points-message'] = 'Invalid timestamp'
            if response_type == 'csv':
                response = make_response( 'pc,400' )
                response.headers["Content-type"] = "text/csv"
                return response
            else:
                return jsonify(**response)
        
        points = [{
            'value': point_value,
            'at': point_at
        }]
        
        points_request['points'] = points
        
        atto_db.do(points_request,'update','points',(config['network-id'],object_id,stream_id),at)
        response.update( atto_db.db_message )
    
    elif request.method == 'DELETE':
        # Delete Points
        # Delete all but N most recent data points (Optional)
        points_except = request.args.get('points-except',None,type=int)
        # Before date/time (Optional)
        before = request.args.get('points-before',None,type=str)
        # After date/time (Optional)
        after = request.args.get('points-after',None,type=str)
        
        # Points delete Input
        points_delete = {}
        if points_except is not None and isinstance(points_except,int):
            points_delete['except'] = points_except
        if before is not None and isinstance(before,str):
            points_delete['before'] = before
        if after is not None and isinstance(after,str):
            points_delete['after'] = after
        
        points_request['points'] = points_delete
        
        atto_db.do(points_request,'delete','points',(config['network-id'],object_id,stream_id),at)
        response.update( atto_db.db_message )
    
    if response_type == 'csv':
        if request.method == 'GET' and response['points-code'] == 200:
            s = "pc,200\n"
            for point in response['points']:
                s += point['at']+","+str(point['value'])+"\n"
            response = make_response(s[:-1])
            response.headers["Content-type"] = "text/csv"
            return response
        else:
            response = make_response( 'pc,'+str(response['points-code']) )
            response.headers["Content-type"] = "text/csv"
            return response
    else:
        return jsonify(**response)


@app.errorhandler(500)
def internal_error(error):
    return jsonify(**{'server-message':'An unknown internal error occured','server-code':500})

@app.errorhandler(404)
def not_found(error):
    return jsonify(**{'server-message':'Not a valid endpoint','server-code':404})
            
# Check if the network exists and create, if necessary
with app.app_context():
    exists, net = atto_db.networkExists((config['network-id'],))
    if not exists:
        # Create the default network
        network_request = {
            'network-id': config['network-id'],
            'network-details': {
                'network-name': 'Local Wallflower.cc Network'
            }
        }
        at = datetime.datetime.utcnow().isoformat() + 'Z'
        atto_db.do(network_request,'create','network',(config['network-id'],),at)
        
if __name__ == '__main__':
    # Start the Flask app
    app.run(host='0.0.0.0',port=config["http_port"])
