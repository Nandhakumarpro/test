import pandas as pd
from pandas import datetime
import json
#from localsWrite import localsWrite as lwrt
Map = lambda x : x['S']
import time
import os
import subprocess
#from pandas.core.groupby.generic import DataFrameGroupBy
from  stringInString import stringInString as S
import mysql.connector as msc
conn = msc.connect( host = 'localhost' , user = 'root' , password ='transpolinux', database = 'test')
#conn = msc.connect( host = 'localhost' , user = 'admin' , password ='transpolinux', database = 'test')
cur = conn.cursor()
LOCALS = False
from multiprocessing import  Pool
dixt = {  }

allDevices =   [ "104" ,"105" , "902" , "903","102" , "103" ,"901" , "113" ]
devicePaired = [
  ["104" , "105" , "000M1D" , "000M1U"],
  ["902" ,"903" , "0019BU" , "0019BD" ],
  ["102" , "103" , "0005AD" , "0005AU"],
  ["901" , "104" , "00M70D" , "00M70U"],
  ["902" , "113" , "00519D" , "00519U"],
  ["901" , "105" , "00570U" , "00570D"]
]


class wifiProject :

    def createDbIfNotAvail(self , devicePair ):
        for each in devicePair:
            Query = wifiProject.createTableQuery(each)[0]
            cur.execute(Query)
        conn.commit()
        if  LOCALS:dixt['CDN'] = locals()
    def poolRead_wms(self , devicePairEach ) :
        q_cond = wifiProject.qcond(devicePairEach , self.time_now , self.time_past  )
        with open('data1_'+devicePairEach+'.json', 'w') as outfile :
            json.dump(q_cond, outfile)

        # query_aws = '~/Music/pypy3.5-v7.0.0-linux64/bin/aws dynamodb query --table-name WMS_COEUT --key-condition-expression "device_id = :did and mac_timestamp BETWEEN :st AND :et" --expression-attribute-values file://data1_'+ devicePairEach+'.json > ' +\
        #             self.filePath+ '/temp_' + devicePairEach + '.json'
        query_aws = 'aws dynamodb query --table-name WMS_COEUT --key-condition-expression "device_id = :did and mac_timestamp BETWEEN :st AND :et" --expression-attribute-values file://data1_' + devicePairEach + '.json > ' + \
                    self.filePath + '/temp_' + devicePairEach + '.json'

        print(query_aws)
        os.system( query_aws )
        #if LOCALS:dixt['prw'] = locals()

    def read_wms(self , allDevices , timeIndex  ):

        self.filePath = wifiProject.filePath()
        self.time_now = time_now =  timeIndex
        self.time_past = time_past =  timeIndex  - 60
        print('FROM :: ' + str(time_past) + ' NOW :: ' + str(time_now))
        p = Pool(processes=  3 )
        p.map( self.poolRead_wms , allDevices )
        p.close()
        if LOCALS:dixt['rms'] = locals()
        #self.dataProcess()
    def mainExecutor(self  , liveTime  ):
       # Devices =
        #for index in range( 1556476200 , 1557081000, 60 )[50:]:
        self.read_wms( allDevices, liveTime )
        self.dataProcess(allDevices)
        for pair in devicePaired:
            self.csvUpdate( pair )
                #print(pair)
                
        #return self.time_now

    def dataProcess(self , allDevices ) :
        #self.D = devicePairedEach
        #self.createDbIfNotAvail(devicePairedEach)
        self.createDbIfNotAvail(allDevices)

        mappedPath = lambda x : [ self.filePath +'/' +'temp_'+x+'.json' ,x]

        for each in list(map(mappedPath , allDevices)) :
            self.excelCreate( each )

        """
        file1path = self.filePath+'/'+'temp_'+self.D[0]+'.json'
        file2path = self.filePath+'/'+'temp_'+self.D[1]+'.json'
        if LOCALS : dixt['dp'] = locals()
        #lwrt(str(locals()))"""
        #self.excelCreate( file1path , file2path )
    def excelCreate(self , file1path ):
        df1 = self.dataFrameGen(file1path[0])
        #df2 = self.dataFrameGen(file2path)

        Groups1 = df1.groupby('mac')
        #Groups2 = df2.groupby('mac')
        if LOCALS : dixt['ec'] = locals()
        self.ConfigureAndExe(Groups1  ,file1path[1])
    def dataFrameGen(self , filePath ):
        with open(filePath , 'r') as file :
            try :
                data = json.loads( file.read())
            except:
                data = { 'Items':[]}
            df = pd.DataFrame(data['Items']  , columns=wifiProject.dataFrameColumns())
            df = df.applymap( Map )
        return  df
    def filterGroupsSub ( self , subDF  ):
        df = subDF
        df = df.sort_values( 'logtime' , ascending= False)
        """
        y = 100000000000

        for key, Frame in df['mac'].iteritems():
            temp = int(float(df.loc[key, 'logtime']))
            df.loc[key, 'Duplicates'] = [True if y - 30 > temp else None][0]
            y = temp
        df = df.dropna().iloc[: , :-1]"""
        return df.iloc[0:1 ]

    def ConfigureAndExe(self ,Groups1  , d ):
        Keys1 = Groups1.groups.keys()  #
        #Keys2 = Groups2.groups.keys()  #
        group1DF = pd.DataFrame(columns=wifiProject.dataFrameColumns())
        #group2DF = pd.DataFrame(columns=wifiProject.dataFrameColumns())
        p = Pool(processes=3)

        self.df1List = p.map(self.filterGroupsSub, [Groups1.get_group(key) for key in Keys1])
        print('first work completed')
        #self.df2List = p.map(self.filterGroupsSub, [Groups2.get_group(key) for key in Keys2])
        p.close()
        print('first pool closed')

        #p = Pool( processes = 3)
        try:
            group1DF = group1DF.append(self.df1List)
        except:
            print('dataframe1 empty')
        """
        try:
            group2DF = group2DF.append(self.df2List)
        except:
            print('dataframe2 empty')
        """
        self.updateDBLeft(group1DF , d )
        #self.updateDBRight(group2DF)
        #p.map(self.updateDBLeft ,  self.df1List )
        #p.map( self.updateDBRight , self.df2List)
        #p.close()
        print('second pool closed')
    def csvUpdate(self , devicePairedEach ):
        self.D = devicePairedEach
        resultQuery1 = wifiProject.resultQuery1(self.D[0] , self.D[1] , self.time_now , self.time_past)
        cur.execute(resultQuery1)
        #data = cur.fetchall()
        data1 = cur.fetchall()
        resultQuery2 = wifiProject.resultQuery2(self.D[0] , self.D[1] , self.time_now , self.time_past)
        #print(resultQuery2)
        cur.execute(resultQuery2)
        data2 = cur.fetchall()
        data = data1 + data2
        #print(data)
        df = pd.DataFrame ( data  , columns = ['mac' , 'logtimeLeft' , 'logtimeRight'])
        #print(df)
        func = lambda x : int(x)
        df['logtimeLeft'] , df['logtimeRight'] = df['logtimeLeft'].map(func) , df['logtimeRight'].map(func)
        df['route_id'] , df['travel_time'] =   [self.D[2] if index else self.D[3] for index in df.logtimeLeft<df.logtimeRight] , [abs(index) for index in df.logtimeLeft - df.logtimeRight]
        datetimeMap = lambda x : datetime.strftime(datetime.fromtimestamp(x) , "%a %b %d %H:%M:%S %Y" )
        dfLeftMap = lambda x : df.loc[x,'logtimeLeft']
        dfRightMap = lambda x : df.loc[x , 'logtimeRight']
        df['datetime'] = [ datetimeMap(dfLeftMap(index)) if dfLeftMap(index) > dfRightMap(index) else datetimeMap(dfRightMap(index))  for index in df.index ]
        df = df.drop(['logtimeLeft' ], axis = 1 )

        df = df[['logtimeRight' , 'datetime' , 'route_id' , 'mac','travel_time']]
        df.columns = ['timestamp','datetime' , 'route_id' ,'mac','travel_time']
        groups = df.groupby("route_id")
        path = wifiProject.csvFilePath()
        left1 = path + f"{self.D[2]}-"+datetime.strftime(datetime.fromtimestamp(self.time_now) , "%d-%m-%y")+".csv"
        right1 = path + f"{self.D[3]}-" + datetime.strftime(datetime.fromtimestamp(self.time_now) , "%d-%m-%y") + ".csv"
        #print(left1)
        #print(right1)

        try:
            if os.path.isfile(left1):
                #print(groups.get_group(self.D[2]))
                groups.get_group(self.D[2]) .to_csv ( left1 ,mode= 'a' , index= False , header= False )
            else:
                groups.get_group(self.D[2]) .to_csv ( left1 ,mode= 'a' , index= False , header= True )

        except:
            print('None')
        try:
            if os.path.isfile(right1):
                groups.get_group(self.D[3]).to_csv(right1, mode='a', index=False, header=False)
            else:
                groups.get_group(self.D[3]).to_csv(right1, mode='a', index=False, header=True)

        except:
            print('None')
        # wifiProject.csvSqlUpdate( groups.get_group(self.D[2]) , self.D[2])
        # wifiProject.csvSqlUpdate(groups.get_group( self.D[3]) , self.D[3])
        conn.commit()
        #print(data)
        #conn.close()
        #resultDf.to_excel( dataPath + '/Results/result'+self.count+'.xls' , sheet_name='sheet1')
    def updateDBLeft ( self , df , d ) :
        for each in df.index :
            temp = df.loc[each]
            sqlQuery = wifiProject.updateQuery(d) +S(temp.device_id)+','+S(temp.mac)+','+temp.logtime+','+temp.mac_timestamp+" , "+S(f'{datetime.strftime(datetime.fromtimestamp(float(temp.logtime)) , "%d-%m,%H:%M:%S")}')+")"
            #print(sqlQuery)
            cur.execute(sqlQuery)
        conn.commit()
    def updateDBRight(self , df ):
        for each in df.index :
            temp = df.loc[each]

            sqlQuery = wifiProject.updateQuery(self.D[1]) +S(temp.device_id)+','+S(temp.mac)+','+temp.logtime+','+temp.mac_timestamp+ " , " +S(f'{datetime.strftime(datetime.fromtimestamp(float(temp.logtime)) , "%d-%m,%H:%M:%S")}')+')'
            #print(sqlQuery)
            cur.execute(sqlQuery)
        conn.commit()
    def printMethod(func ):
        def wrapper( *args , **kwargs ):
            y = func(*args , **kwargs )
            print(y)
            return y
        return wrapper
    def filePath():
        path1 = ['..','supportFiles' , 'Device']
        path2 = [ ".." , 'tempFiles' , 'Device']
        return '/'.join( path1 
