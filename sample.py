from flask_pymongo  import PyMongo
from flask import Flask , render_template , request , jsonify
import bcrypt
import json
import ast
import jwt
from functools import  wraps
import datetime
import time
import pickle
import secrets
import numpy as np

# testTable = mongo.db.testDevice
userTable = mongo.db.userAuth
RaspRegTable = mongo.db.RaspAuth

@app.route('/getTime')
def getTime() : 
  return str( time.time())

def stringInString( data ):

    data = json.dumps( data )
    return '\''+ str(data)+'\''

def bytesToData(bI):
#     print(type(bI))
#     data = bI.decode('utf-8')
    # print( type( data) , data )
#     data = json.loads( json.dumps( ast.literal_eval( data  )))
#     print(type( pickle.loads(bI))) 
    # print( type( data) , data )
    return pickle.loads(bI)

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
  
def updateTokenRequired(f):
  @wraps( f )
  def decorated(*args , **kwargs ) :
    try:
      rn31 = pickle.loads(request.data.get('rn31'))
      rn32= pickle.loads(request.data.get('rn32'))
      O1 = pickle.lodas(request.data.get('O1'))
      O2 = pickle.lodas(request.data.get('O2'))
      x1 = np.linalg.solve( rn31 , O1)
      x2 = np.linalg.solve(rn32 , O2)
      Count = pickle.loads(request.data.get('Count'))
      Comb = np.hstack([x1 , x2] )
      Comb = list ( map( lambda x : int(round(x)) , Comb))
      temp =Comb.copy()
      for i,j in enumerate(Count) :
        temp[j] = str(Comb[i])
      if float(str(pd.datetime.now().timestamp())[4:])> int(''.join(temp)) > float(str(pd.datetime.now().timestamp())[4:])-60  :
        return f(*args , **kwargs)
      else:
        return 'token is invalid'
    except:
      return 'token is invalid2'
  return decorated

@app.route( '/signup')
def signup(  ):
    return render_template('signup.html')

@app.route( '/login')
def login( ):
    return render_template( 'login.html' )
    
app.run ( )
