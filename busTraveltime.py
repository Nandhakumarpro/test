import os
# %matplotlib qt
import pandas as pd
import pickle
import numpy as np
import osmnx as ox
import networkx as nx
from IPython.display import display

from pymongo import MongoClient
S = MongoClient ( 'localhost' , 32017 )
db = S.busData
table = db[ 'busDataToNodeDataJuly' ]
TravelTimeDB = db [ 'ttTableJuly']
from multiprocessing import Pool

class bus_ttFromGpsData :
    def __init__( self , defltGrp = True , **kwargs ) :
#         self.createTable()
        self.leftRefPoint =  [12.9538786, 80.1923717]
        self.rightRefPoint  = [ 13.0246025  , 80.1923717 ]
        with open( './localObjects/bus_IDs.pickle' , 'rb' ) as file :
            self.bus_ids = pickle.loads( file.read ( ) ) 
        if defltGrp :
#             with open ( './localObjects/graph.pickle' , 'rb' ) as graph :
                
            with open ( './localObjects/filGraph.pickle' , 'rb' ) as graph :
            
                print ( '##running##' )
                self.graph = pickle.loads ( graph.read() )
        else:
            self.graph = kwargs.get ( 'defltGrp' )
            if not self.graph :
                print ( 'Please  Load  Graph ' )
        # print ( 'running' )
    def getNodesAndEdges ( self ) :
        nodes , edges = ox.graph_to_gdfs ( self.graph )   
        return nodes , edges 
    @staticmethod 
    def haverSine ( coOrd1 , coOrd2  ) :
        delLat , delLong = np.array ( coOrd1 ) - np.array ( coOrd2 )
        delLatRad , delLongRad = np.radians ( delLat ) , np.radians ( delLong )
        Lat1Rad , Lat2Rad = np.radians( np.array ([ coOrd1[0] , coOrd2[0] ])) 
        a = (np.sin( delLatRad /2  ))  **2 + (np.cos (Lat1Rad ) * np.cos ( Lat2Rad ) * (np.sin (delLongRad/ 2  ))**2  )
        c = 2 * np.arctan2( np.sqrt ( a) , np.sqrt( 1 - a ) )
        d = 6371 * 1000 * c 
        return  d 

class bus_ttFromGpsData ( bus_ttFromGpsData ) : 
    def printMethod(f):
      def wrapper ( *args , **kwargs ) :
        print ( 'running')
        y = f( *args , **kwargs )
        if type(y) == pd.core.frame.DataFrame : 
  #         print( 'Display Loop')
          display( y.head(3))
          print ( y.info() )
        elif type ( y) == list :
          list ( map ( bus_ttFromGpsData.printMethod ( lambda x: x)  , y  ) )
        else :
  #         print ( type ( y ))
          print( y )
        return y 
      return wrapper
    @staticmethod
    def createTable (  ) :
        cur.execute ("""
        create table if not exists busDataToNodeData (  ID INT auto_increment , Date varchar(20) , Time varchar(20),
        Latitude float ( 20,8 ) , Longitude float(20 ,8 ) ,timestamp float( 20 , 5 ) , nodeTimeStamp float (20 , 5),  nearestNodeLat float(20 ,8),  nearestNodeLong float(20 ,8), busID varchar ( 200 ) , primary key(ID))""")
    @staticmethod
    def insertData ( data  ) :
        # f = lambda y : cur.execute ("""insert into busDataToNodeData ( Date,Time,Longitude,Latitude,timestamp,nodeTimeStamp,nearestNodeLat,nearestNodeLong  )   values( {} ,{} ,{} , {} , {} , {} , {} , {} ) """.format  ( *list ( map (  lambda x :json.dumps(x), y ))))
        # list ( map ( f , data.values ) )
        # ###   data Type  = List
        cur.executemany ("""insert into busDataToNodeData ( Date,Time,Latitude,Longitude,timestamp,nodeTimeStamp,nearestNodeLat,nearestNodeLong , busID ) values( %s , %s , %s , %s , %s , %s , %s , %s , %s ) """ , data ) 
        con.commit()
    @staticmethod
    def insertDataToMongoDB ( data  ) :
      table.insert_many ( data )
      table.commit
    @printMethod
    def main ( self  , fileLocation ) :
#         p = Pool ( 2 )
        busDataFrame = self.busDataFrame ( fileLocation )
        busDataFrame.dropna ( inplace = True )
#         busDataFrame[ 'nearestNodes' ] = list ( map (  self.getNearestNodeLatLong , busDataFrame.iloc[ : , [ 2,3]].values ))
        nodes_df = self.getNodesAndEdges ()[0] 
        with Pool ( 6 ) as p:
          busDataFrame[ 'nearestNodes' ] =  p.map ( self.getNearestNodeLatLong  , busDataFrame.iloc[ : , [ 2,3]].values  ) #[ self.getNearestNodeLatLong ( i , graph = self.graph , nodes_df = nodes_df  )  for i in busDataFrame.iloc[ : , [ 2,3]].values  ]
        busDataFrame[ 'timestamp' ] = [pd.Timestamp ( x ).timestamp() - 19800  for x in   busDataFrame.Date + ' ' + busDataFrame.Time  ]
        bdf = busDataFrame
        
        bdf['nearestNodesCompare'] = None

        bdf.iloc[:-1 , -1 ] =  ( bdf.nearestNodes.values [ : -1  ] == bdf.nearestNodes.values [ 1:  ] )
        Splitter = self.mainSplitter  ( bdf.nearestNodesCompare.values )
        bdf[ 'nodeTimeStamp' ] = None
        for each in Splitter :
          if  (each[1] -  each [ 0 ] ) > 1 : 
            R = self.ttInterpolationPoints ( dataValues=bdf.iloc [ each[0] : each[1], 2:4 ].values  , nearestNodePoint= bdf.nearestNodes.values[each[0]]  ,  middle= x.middle ( bdf.iloc [ each[0] : each[1], 2:4 ].values ) , a3=x.Angle ( bdf.nearestNodes.values[each[0]] , leftRefPoint = x.leftRefPoint , rightRefPoint = x.rightRefPoint  ))    
            pre , curr = bdf.iloc [ R[0] + each[0] ] , bdf.iloc [ R[0]+each[0] + 1 ]
          else :
            bdf.iloc [ each [0] , -1 ] = bdf.iloc [ each[0] ].loc[ 'timestamp' ]
            continue
#           return  pre , curr 
          bdf.iloc[ R[0] + each[0] , -1 ] = self.interpolationForTimestamp ( pre.nearestNodes , curr.nearestNodes , pre.loc  [['Latitude' , 'Longitude']] , curr.loc [ ['Latitude' , 'Longitude'] ]  , pre.timestamp , curr.timestamp )
        
#         bdf.nodeTimeStamp.values[ :-1] = list (map ( self.interpolationForTimestamp  , bdf.nearestNodes.values[:-1] , bdf.nearestNodes.values[1:] , bdf.iloc[ :-1 , [2,3]].values , bdf.iloc[1: , [2,3]].values , bdf.timestamp.values[:-1] , bdf.timestamp.values[1:]  )  )
#         bdf.nodeTimeStamp.values[ :-1] =  [ self.interpolationForTimestamp ( i1, i2 , i3, i4  ,i5 ,i6 , self.haverSine ) for i1 , i2 , i3 , i4 , i5 ,i6 in zip ( bdf.nearestNodes.values[:-1] , bdf.nearestNodes.values[1:] , bdf.iloc[ :-1 , [2,3]].values , bdf.iloc[1: , [2,3]].values , bdf.timestamp.values[:-1] , bdf.timestamp.values[1:]  ) ] 
        bdf[ 'nearestNodeLat' ] = bdf.nearestNodes.apply( lambda x:x[0])
        bdf[ 'nearestNodeLong' ] = bdf.nearestNodes.apply( lambda x:x[1])       
        bdf.drop( ['nearestNodes']  , axis = 1 , inplace = True )
        bdf [ 'busID'] = os.path.basename ( fileLocation ) [ :-13]
        bdf.dropna ( inplace = True )
        print ( 'DataFrame Work Completed ' )
        return bdf
    
    def mainExe( self , homeDir) :
        path = os.path.abspath  ( homeDir )

        for each1 in os.listdir (path) :
          for each in os.listdir ( path+'/'+each1 ):
#           self.insertData ( self.main ( path + '/' + each ).values.tolist() )
            data = self.main ( path + '/' + each1 +'/'+each )

            if not data.empty:
              self.insertDataToMongoDB ( data.to_dict( orient = 'records' ) )


    @staticmethod
    def busDataFrame ( fileLocation ) :
        return pd.read_csv( fileLocation )

        
    # def getNearestNodeLatLong ( coOrdInput , graph=graph , node_df = nodes ) : 
    #     osmID = ox.get_nearest_node ( graph , coOrdInput  ) 
    #     t = node_df [ node_df.osmid == osmID  ];return t.y.values[0] , t.x.values[0]    
        

    
    @staticmethod
    def getNearestNodeLatLong ( coOrdInput , graph = bus_ttFromGpsData().graph , nodes_df = bus_ttFromGpsData().getNodesAndEdges()[0]  ) :
        osmID = ox.get_nearest_node ( graph , coOrdInput )
        t = nodes_df [ nodes_df.osmid == osmID ]
#         print ( t , type(t ) )
        try:
          return t.y.values[0] , t.x.values[0]
        except:
          return np.nan


    @staticmethod
    def interpolationForTimestamp (  preNearestNodePoint , currNearestNodePoint , preBusPoint, curBusPoint , preTimestamp , currTimestamp , outSide = [False ,0]  , haverSine = bus_ttFromGpsData.haverSine )  :
        
#         if list (preNearestNodePoint)  !=  list (currNearestNodePoint ) :
        if not outSide [0 ] : 
            return (( ( currTimestamp - preTimestamp ) / haverSine ( preBusPoint  , curBusPoint  ) ) * ( haverSine ( preBusPoint  , preNearestNodePoint ) )  ) + preTimestamp    
        else :
            return ( outSide [1]  * (( ( currTimestamp - preTimestamp ) / haverSine ( preBusPoint  , curBusPoint  ) ) * ( haverSine ( preBusPoint  , preNearestNodePoint ) )  ) ) + preTimestamp 
        
    @staticmethod
    def Angle ( x1 , rightRefPoint = bus_ttFromGpsData().rightRefPoint  , leftRefPoint = bus_ttFromGpsData().leftRefPoint , d =  7864.138872517201 ) : 
      dbrr = x.haverSine ( x1 , rightRefPoint )
      dblr = x.haverSine( x1 , leftRefPoint )
#       print ( dbrr , dblr )
      Angle = np.rad2deg ( np.arccos (  (( dbrr **2  ) +  ( d  **2 ) - ( dblr **2 )) /  ( 2 * dbrr * d )))
    #   print (  np.arccos (  ) / ( 2 * dbrr *d ) )
      print( )
      return Angle
    @staticmethod
    def mainSplitter ( x  ) :
      R = [ ]
      L = len( x )
      start = 0
      while start < L :
    #     print ( 'running' )
        try :
          y = list ( np.array ( x ) [start : ] )
          R.append ( [ start , start+ y.index( False ) + 1  ] )

          start = start+ y.index( False ) +1
        except Exception as e :
          R.append ( [ start  , len ( x ) - 1 ] )
          break

      return R
    @staticmethod
    def middle ( x )  :
      if len ( x) != 1 :
        return (len ( x ) -1 )// 2  #, ((len (x ) -1  ) // 2) + 1
      else :
        return 0
    @staticmethod
    def ttInterpolationPoints ( dataValues , nearestNodePoint  ,middle  , a3 ) :
      leftMove , rightMove = 0 , 0
      while ( ( middle  < len ( dataValues ) - 1 )   &  (  middle > -1  ) ) :
        a1 = bus_ttFromGpsData.Angle ( dataValues [ middle] )
        a2 = bus_ttFromGpsData.Angle ( dataValues [ middle + 1 ] )
        a3 =  a3 #Angle (  nearestNodePoint )
        if a1 > a3 :
          if a3 > a2 :
            return middle , middle + 1
          if a1 > a2  :
            middle = middle + 1 
            leftMove , leftValue = True , middle

          else :
            middle = middle - 1 
            rightMove , rightValue  = True , middle
        else :
          if a3 < a2 :
            return  middle , middle + 1
          if a1 < a2 :
            middle = middle + 1
            leftMove , leftValue = True , middle
          else : 
            middle = middle - 1
            rightMove , rightValue = True , middle
        if ( leftMove & rightMove ) :
          return leftValue , rightValue

      else:

        if middle == -1 :
          return middle +1 , middle + 2 , [ True , int (( np.sign ( dataValues [middle +1] - dataValues [ middle + 2 ] )[0]) ) ]
        else :
          return middle , middle - 1 , [ True , int ( np.sign(( dataValues [ middle  ] - dataValues [ middle -1 ] )[0])  ) ]
#     @staticmethod       
#     @printMethod
#     @printMethod  
    def getTraveltime( self , routeNodes  , timeStamp , route ) :
      combFrame = self.getDataFromMongoDB ( routeNodes , timeStamp=timeStamp )
      busInSlot = combFrame.busId.unique()
      ResultFrame = pd.DataFrame ( columns = [ 'traveltime'] )
      for each in list( busInSlot ) :
        tmp = combFrame.groupby( 'busId' ).get_group ( each )
        tmp.timeStamp = tmp.timeStamp.apply( lambda x: [ x ] )
        r = self.averageTTCheck ( self.ttInterpolateFunc ( tmp.loc [ : , [ 'NodeLatLong' , 'timeStamp' ]].values , distance = self.haverSine(routeNodes[0] , routeNodes [-1] )  )   )
        ResultFrame.loc [ each ] = r
      print( ResultFrame )  
      if not ResultFrame.query( '0 < traveltime' ).empty  :
        TravelTimeDB.insert_one ( { 'timestamp' :timeStamp , 'route' : route +'d' , 'tt' :ResultFrame.query( '0 < traveltime ' ).traveltime.mean()  } )
      if not ResultFrame.query ( '0 > traveltime' ).empty :
        TravelTimeDB.insert_one ( {'timestamp' :timeStamp , 'route' : route +'u' , 'tt' : -1 * (ResultFrame.query( '0 >  traveltime ' ).traveltime.mean())  } )
      return ResultFrame.traveltime.mean()  
    
        
    @staticmethod
    @printMethod
    def getDataFromMongoDB ( routeNodes , timeStamp ) :
      Result =  [ ]
      ResultFrame = pd.DataFrame ( columns= [ 'busId' , 'timeStamp' ,'NodeLatLong'  ] )
#       dfData = pd.DataFrame ( )
      dataList = [ ] ; print ( 'modified' );
      dfData = pd.DataFrame ( list ( table.find (  { 'nodeTimeStamp':{'$gte': timeStamp - 900 , '$lte': timeStamp }  } , { 'nodeTimeStamp':1 , 'busID':1 , 'nearestNodeLat':1 , 'nearestNodeLong':1,'_id':False }  )))
      print ( dfData )
      for each in routeNodes:
        Tempdata = dfData .query ( '(@each[0]==nearestNodeLat) & (@each[1]==nearestNodeLong)' ) 
#         print ( data )
#         dataList.append ( pd.DataFrame ( data  ) )
#         dfData = pd.DataFrame ( data )
        if not Tempdata.empty :
          for i in Tempdata.values :
            Result.append( list (i ) + [ each ] )
      if Result:
        ResultFrame = pd.DataFrame ( Result  )
        ResultFrame = ResultFrame .iloc[ : , [ 0 ,3,4] ]
        ResultFrame.columns = [ 'busId' , 'timeStamp' ,'NodeLatLong'  ]
      return ResultFrame 
   
    @staticmethod
    @printMethod
    def ttInterpolateFunc ( x  , distance  ) : 
      left , right = 0 , 1
      main = [ ]
      while right <  ( len ( x) ) :
        print ( right , len ( x ) )
    #     print ( left , right )
        try:
          main .append([  bus_ttFromGpsData.haverSine ( x[right][0] ,  x[ left][0] )  ,  x[right][ 1 ] [0 ] - x[ left ] [ 1 ] [0] ])
#           print ( 'next to main' )
          left = left + 1
          right  = right + 1
        except  Exception as e :
          print ( e )
          if x[ right ][ 1 ] == [] :
            right = right + 1
          if x[left ] [1] == []  :
            left = left+ 1
            if left ==  right :
              right = right + 1 
      return main , distance #bus_ttFromGpsData.haverSine ( x [-1] [0 ]  ,  x[ 0 ] [ 0] )
    @staticmethod
    @printMethod
    def averageTT ( i , j ) :
      i = np.array ( i )
      isum = i[: , 0].sum ()
      iright  = i[ : ,1 ]
      ileft = i[ : , 0 ]
      return (( (iright/ ileft) * j  )*( ileft/ isum ) ).sum()
    @staticmethod
    @printMethod
    def averageTTCheck( x ):
      try :
        return bus_ttFromGpsData.averageTT ( *x)
      except Exception as e:
        print ( e )
        return np.nan
      
    @staticmethod
    def getNearestNodesForRouteByMongo ( lat1 , lat2 , long1 , long2  , routeBus ) :
      BusDF = pd.DataFrame ( list ( table.find( {'busID':routeBus} , {'_id':False } ) ) ) 
      print ( BusDF.head (  ) )
      R1 = BusDF.query ( '( @lat1 < Latitude < @lat2+0.002 ) & ( @long1 < Longitude < @long2+0.002 ) ' ).loc [: ,  ['nearestNodeLat' , 'nearestNodeLong']]
      R1.set_index ( 'nearestNodeLat' , inplace= True )
      Nodes = []
      for each in list ( R1.index .unique()) :
        try :
          Nodes.append ( [each , R1.loc [ each , ].iloc[: , 0].unique()[0]])      
        except Exception as e:
          print ( e )
          continue
      return Nodes
