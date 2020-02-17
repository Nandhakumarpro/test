from flask_pymongo  import PyMongo
from flask import Flask , render_template , request
import bcrypt
import json
import ast
import jwt
from functools import  wraps
import datetime
import time
app = Flask( __name__ , template_folder = '/templates' )
mongo = PyMongo ( app )
print  ( "This is updated " )
testTable = mongo.db.testTable
userTable = mongo.db.users

def stringInString( data ):

    data = json.dumps( data )
    return '\''+ str(data)+'\''

def bytesToData(bI):
    data = bI.decode('utf-8')
    # print( type( data) , data )
    data = json.loads( json.dumps( ast.literal_eval( data  )))
    return data

def hassPD(I:str):
    return bcrypt.hashpw( I.encode() , bcrypt.gensalt())

def tokenrequired(f):
    @wraps( f )
    def decorated ( *args , **kwargs ):
        token = request.args.get( 'token')
        if token :
            try:
                jwt.decode ( token , app.config['secretkey'] )

            except:
                return 'your token has expired'

        else:
            return 'token is missing'

        return f( *args , **kwargs )
    return decorated

@app.route( '/signup')
def signup(  ):
    return render_template('signup.html')

@app.route( '/login')
def login( ):
    return render_template( 'login.html' )

@app.route( '/loginvalid' , methods = ['POST'])
def loginvalid():
    if userTable.find_one( {'user':request.form['userid']}):
        if bcrypt.checkpw( request.form['pwd'].encode('utf-8') ,   userTable.find_one({'user':request.form['userid']}).get('password')):
            if time.time() > userTable.find_one({'user':request.form['userid']}).get('logoutTime'):
                dict = userTable.find_one({'user': request.form['userid']})
                userTable.update_one({'user':request.form['userid']} , {"$set":{'logoutTime':time.time() + dict["Session"] }})
                return gettoken(dict['user'] , dict['Session'])
            else:
                return 'You are already logged'
        else:
            return 'please enter the right password'
    else:
        return 'You are not registered'

@app.route('/signupvalid' , methods = ['POST' ])
def signupvalid():
    hashP = lambda password : bcrypt.hashpw( password.encode('utf-8') , bcrypt.gensalt( ))
    if userTable.find_one( {'user':request.form['userid']}):
        if not userTable.find_one({'user':request.form['userid']}).get('password'):
            userTable.update_one( {'user':request.form['userid']} , {"$set":{'password': hashP(request.form['pwd'])}})
            return 'Successfully Registered'
        else:
            return 'UserName already registered'
    else:
        return 'You are not Authorised to enter'
