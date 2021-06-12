import ftplib
import subprocess
import sys
from datetime import datetime
from datetime import timedelta
from dateutil.parser import parse as dateparser
import time
import json
import logging
import ast
import requests
import re
import os
import uuid
import glob
import hashlib
try:
    import jaydebeapi
except:
    print("no jaydebeapi")
import urllib3
import traceback
from google.protobuf import json_format
from google.transit import gtfs_realtime_pb2
from enum import Enum
import inspect
try:
    import memory_profiler
except:
    print("no memory_profiler")
from functools import partial, wraps
try:
    import xlrd
except:
    print( "no xlrd" )

# ORACLE
try:
    import cx_Oracle
except ImportError:
    pass

# HDFS
try:
    from hdfs import InsecureClient
except ImportError:
    pass

# ADL
try:
    from azure.datalake.store import core, lib, multithread
except ImportError:
    pass

# SOAP
try:
    from zeep import Client, Transport
    import zeep
except ImportError:
    pass

# SPARK
try:
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import HiveContext, DataFrame, SparkSession, SQLContext
except ImportError:
    pass

# KAFKA
try:
    from kafka import KafkaConsumer
    from kafka import KafkaClient
    from kafka import *
except ImportError:
    pass

# per usare la classe fare i seguenti import:
# from utility import Common
# from utility import Logger
# from utility import Spark
# from utility import Hdfs


"""
v 2.2
- aggiunta hdfs_merge_files
- modifica copyLogHdfs
- aggiunta classe logger
- aggiunto in common.initializeVar() parametro per sezione e gestione schema dataframe (StructType) #GV
- modifica executeMany nella classe CxOracle
- aggiunto timoeut request

v 2.3 Antonino
- aggiunta set_key_value() in DbCxOracle
- aggiunta get_key_value() in DbCxOracle
- aggiunta campo 'info' in class ErrorException
- modifica request in class Http -> raise ErrorException(..., info=response)
- aggiuna func warning() in Logger

v 2.3.1 Giuseppe
- aggiunta "encoding="UTF-8", nencoding="UTF-8"" in connect in DbCxOracle

v 2.3.2 Antonino
- aggiunta getNumExecution() in Common
- aggiunta roundDate() in Common
- aggiunta parametro "parameters" in executeProcedure() di DBoracle
- aggiunta union_date_intervals() in Common

v 2.3.3 Michele
- modifica funzione getDfFromQuery da Oracle

v 2.3.4 Giuseppe Iossa
- aggiunta della classe FieldControl
- aggiunta della classe Adl

v 2.3.5 Antonino
- aggiunta classe Compattatore

v 2.3.6 Michele
- aggiunta hdfsMv nella classe Hdfs

v 2.3.7 Antonino
- aggiunta classe Compattatore (nuova versione)

v 2.3.8 Giuseppe
- modifica logEndExecution (scrittura in nuova tabella log_hdfs)
"""

def encryptStr(value):
    result = hashlib.sha256(value.encode())
    return result.hexdigest() 

class Adl:
    def __init__(self, store_name = None, tenant = None, client_secret = None, client_id = None, resource = None):
        self.creds = lib.auth(tenant_id = tenant, client_secret = client_secret, client_id = client_id, resource = resource )
        self.adlsFileSystemClient = core.AzureDLFileSystem(self.creds, store_name = store_name)
        auth_url = "https://login.microsoftonline.com/" + tenant + "/oauth2/token"
        body = {"grant_type": "client_credentials",  "resource": resource,  "client_id": client_id, "client_secret": client_secret }
        self.auth = requests.post(url=auth_url, data=body)

    def getAuthHeaders(self):
        content = json.loads(self.auth.content)
        access_token = content['token_type'] + " " + content['access_token']
        headers = {'Authorization': access_token}
        return headers
		
    def download_from_adl_api(self, download_url):
        # multithread.ADLDownloader(self.adlsFileSystemClient, lpath=lpath, rpath= rpath, nthreads=nthreads, overwrite=True)
        res = requests.get(url=download_url, headers=self.getAuthHeaders())
        return res.text

    def upload_to_adl_api(self, upload_url,mydata):
        # multithread.ADLUploader(self.adlsFileSystemClient, lpath= lpath, rpath= rpath, nthreads= nthreads, overwrite=True)
        res = requests.put(url = upload_url, json = mydata, headers = self.getAuthHeaders())
        return res.text

    def upload_to_adl(self, lpath, rpath, nthreads = 64):
        return multithread.ADLUploader(self.adlsFileSystemClient, lpath = lpath, rpath = rpath, nthreads = nthreads, overwrite = True)

class Http:
    def request(self, url, type="JSON", method="GET", payload=None, timeout=40):
        timeout = 40 if timeout is None else None
        http = urllib3.PoolManager()
        headers = None

        if type == "JSON":
            headers = {"Content-Type": "application/json", "Accept": "application/json"}

        if method == "POST":
            encoded_data = json.dumps( payload ) if payload is not None else None
            response = http.request( 'POST', url, body=encoded_data, headers=headers, timeout=timeout)
        else:
            response = http.request( 'GET', url, headers=headers, timeout=timeout)

        if (response.status > 199 and response.status < 300):
            return response.data
        else:
            e = Exception( "Errore HTTP {}".format( response.status ) )
            raise ErrorException( exception=e, code=ErrorException.APPLICATION_EXECUTION_ERROR, info=response )


class Ftp:
    import ftplib as ftplib_
    import io as io_

    def __init__(self, host, username, password):
        self.ftp_client = self.ftplib_.FTP(host, username, password)

    def getFilenameList(self, directory=None) -> list:
        try:
            return self.ftp_client.nlst("" if directory is None else directory)
        except ftplib.error_perm as error:
            if str(error) == "550 No files found":
                return []
            if str(error) == "550 Directory not found":
                return None
            else:
                raise

    def retriveBinaryFile(self, filename, blocksize=8192, rest=None) -> bytes:
        try:
            r = self.io_.BytesIO()
            self.ftp_client.retrbinary("RETR " + filename, r.write, blocksize=blocksize, rest=rest)
            r.seek(0)
            return r.read()
        except ftplib.error_perm as error:
            raise Exception("ERROR FTP retriveFileBinary: {}".format(error))

    def retriveBinaryFileList(self, files_name, blocksize=8192, rest=None) -> list:
        binary_files = []
        for file in files_name:
            try:
                binary_files.append(self.retriveBinaryFile(file, blocksize, rest))
            except Exception as e:
                binary_files.append(None)

        return binary_files

    def retriveBinaryFilesFromFolder(self, folder_name=None, filename_filter=None, blocksize=8192, rest=None) -> list:
        try:
            files_name = self.getFilenameList(folder_name)
            if filename_filter not in (None, "*"):
                files_name = list(filter(lambda name: re.search(filename_filter, name), files_name))

            return self.retriveBinaryFileList(files_name, blocksize, rest)
        except Exception as e:
            raise

    def retriveFilenameAndBinaryFilesFromFolder(self, folder_name=None, filename_filter=None, blocksize=8192, rest=None) -> list:
        try:
            files_name = self.getFilenameList(folder_name)
            if filename_filter not in (None, "*"):
                files_name = list(filter(lambda name: re.search(filename_filter, name), files_name))

            binary_files = self.retriveBinaryFileList(files_name, blocksize, rest)

            return [dict(filename=name, content=data) for name, data in zip(files_name, binary_files)]
        except Exception as e:
            raise


class SshClient:
    import paramiko as paramiko_
    isConnected = False

    def __init__(self, host, user, password, port):
        self.host = host
        self.user = user
        self.password = password
        self.port = port

        self.ssh = self.paramiko_.SSHClient()
        self.ssh.set_missing_host_key_policy(
            self.paramiko_.AutoAddPolicy())  # Set policy to use when connecting to servers without a known host key

    def openConnection(self):
        if not self.isConnected:
            self.ssh.connect(hostname=self.host, username=self.user, password=self.password, port=self.port)
            self.isConnected = True
            # gestisci eccezioni

    def closeConnection(self):
        self.ssh.close() if self.isConnected else None
        self.isConnected = False

    def getFileContentSFTP(self, filename: str) -> bytes:
        try:
            self.openConnection()
            with self.ssh.open_sftp() as sftp:
                with sftp.file(filename, mode='rb') as file:
                    return file.read()
        except Exception as e:
            raise e  # gestisci le varie eccezioni
        finally:
            self.closeConnection()

    def getFileInformationsSFTP(self, file_path: str) -> dict:
        try:
            self.openConnection()
            with self.ssh.open_sftp() as sftp:
                info = sftp.lstat(file_path)
                return dict(last_update=info.st_mtime, last_access=info.st_atime, protection_bits=info.st_mode,
                            group_id_owner=info.st_gid, user_id_owner=info.st_uid, file_size=info.st_size)
        except Exception as e:
            raise e  # gestisci le varie eccezioni
        finally:
            self.closeConnection()

    def getFilenameListSFTP(self, folder_path: str=None) -> list:
        try:
            self.openConnection()
            with self.ssh.open_sftp() as sftp:
                return sftp.listdir(path='.' if folder_path is None else folder_path)
        except Exception as e:
            raise e  # gestisci le varie eccezioni
        finally:
            self.closeConnection()


class MyTimer:

    def __init__(self, id_=""):
        self.time = time.time
        self.id = id_
        self.last_tic = self.time()
        self.last_toc = self.time()
        self.start_time = self.time()
        self.start_dt = datetime.fromtimestamp( self.start_time )

    def printTime(self):
        print( self.time() )

    def tic(self):
        self.last_tic = self.time()

    def toc(self):
        self.last_toc = self.time()
        return self.last_toc - self.last_tic

    def timeSinceStart(self):
        return self.time() - self.start_time

    def yieldTime(self):
        while True:
            yield self.time() - self.last_tic


class CompattatoreOld:
    FILE_FORMAT_ORC = "orc"
    FILE_FORMAT_PARQUET = "parquet"

    def __init__(self, table_final_location, location_tmp_compact, location_tmp_file, spark=None, hdfs=None):
        try:
            self.table_final_location = table_final_location
            self.location_tmp_compact = location_tmp_compact
            self.location_tmp_file = location_tmp_file
            self.hdfs = hdfs if hdfs is not None else Hdfs()
            if spark is None:
                self.sparkUtility = Spark("compattatore", {"hive.exec.max.dynamic.partition.pernode": "10000"})
                self.flag_spark = True
            else:
                self.sparkUtility = spark
                self.flag_spark = False
            self.SQLContext = self.sparkUtility.getSQLContext()
        except Exception as e:
            traceback.print_exc()
            
    def setLocations(self, table_final_location=None, location_tmp_compact=None, location_tmp_file=None):
        self.table_final_location = table_final_location if table_final_location is not None else self.table_final_location
        self.location_tmp_compact = location_tmp_compact if location_tmp_compact is not None else self.location_tmp_compact
        self.location_tmp_file = location_tmp_file if location_tmp_file is not None else self.location_tmp_file

    # Funzione che effettua la count nelle cartelle temporanee
    def count(self):
        count_compact = subprocess.Popen('hdfs dfs -ls -q {}/*|wc -l'.format(self.location_tmp_compact),
                                         stdout=subprocess.PIPE, stderr=None, shell=True)
        count_compact = count_compact.communicate()
        count_file = subprocess.Popen('hdfs dfs -ls -q {}/*|wc -l'.format(self.location_tmp_file),
                                      stdout=subprocess.PIPE, stderr=None, shell=True)
        count_file = count_file.communicate()
        return int(count_compact[0]), int(count_file[0])

    # Controllo sull'esistenza di un compact nella cartella temporanea. Se esiste già un file, viene lanciata un'eccezione e non va piu avanti (male)
    # aggiusta
    def check(self):
        try:
            #print("********INIZIO CHECK***********")
            subprocess.check_output('hdfs dfs -ls -q {}'.format(self.location_tmp_compact), shell=True,
                                    stderr=subprocess.STDOUT)
            subprocess.check_output('hdfs dfs -ls -q {}'.format(self.location_tmp_file), shell=True,
                                    stderr=subprocess.STDOUT)
            count_compact, count_file = self.count()
            #print("***", count_compact, count_file)
            if (count_compact > 0 or count_file > 0):
                raise Exception("Qualche compattamento nei giorni precedenti è fallito")
            #print("********FINE CHECK***********")
        except(Exception) as e:
            print("********ERRORE CHECK***********")
            raise

            # Creazione della cartella temporanea (/compact) dove viene salvato il file contenente tutti i dati compattati

    def compattamentoFile(self, formato=FILE_FORMAT_PARQUET):
        try:
            #print("********INIZIO COMPATTAMENTO***********")

            # self.final_location_str = "{}/year={}/month={}/day={}".format(self.table_final_location, self.year, self.month, self.day)
            # parquet_df = self.SQLContext.read.format("parquet").load(self.final_location_str)
            parquet_df = self.SQLContext.read.format(formato).load(self.table_final_location)
            parquet_df.coalesce(1).write.mode("append").format(formato).parquet(self.location_tmp_compact)
            #print("********FINE compattamentoFile***********")
        except Exception as e:
            print("********ERRORE compattamentoFile***********")
            raise

    # Creazione della cartella temporanea (/file) dove vengono copiati i file "piccolini"
    def copyFile(self):
        try:
            #print("********INIZIO COPY***********")
            # file_copy = "{}/part*.snappy.parquet".format(self.final_location_str)
            file_copy = "{}/part*.snappy.parquet".format(self.table_final_location)
            result_copy = subprocess.check_call(['hdfs dfs -cp {} {}'.format(file_copy, self.location_tmp_file)],
                                                shell=True, stdin=subprocess.PIPE)
            #print("********FINE COPY***********")
        except Exception as e:
            print("********ERRORE COPY***********")
            raise

    # Cancellazione dei file "piccolini" nella partizione e spostamento del compact
    def delete_move(self):
        try:
            #print("********INIZIO DELETE_MOVE***********")
            # parquet_delete = "{}/part*.snappy.parquet".format(self.final_location_str)
            parquet_delete = "{}/part*.snappy.parquet".format(self.table_final_location)
            #parquet_delete = "{}".format(self.table_final_location) PROVA!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            result_delete = subprocess.check_call('hdfs dfs -rm -r {}'.format(parquet_delete), shell=True,
                                                  stdin=subprocess.PIPE)

            #self.hdfs.hdfsMkdir(self.table_final_location) PROVA !!!!!!!!!!!!!!!!!!!!!!!!
            loc_app = "{}/*.snappy.parquet".format(self.location_tmp_compact)
            # result_move = subprocess.check_call('hdfs dfs -mv {} {}'.format(loc_app,self.final_location_str), shell=True, stdin=subprocess.PIPE)
            result_move = subprocess.check_call('hdfs dfs -mv {} {}'.format(loc_app, self.table_final_location),
                                                shell=True, stdin=subprocess.PIPE)
            #print("********FINE DELETE_MOVE***********")
        except Exception as e:
            print("********ERRORE DELETE_MOVE***********")
            raise

            # Cancellazione tmp

    def remove_tmp(self):
        try:
            #print("********INIZIO REMOVE_TMP***********")
            delete_tmp_compact = subprocess.check_call('hdfs dfs -rm -r {}'.format(self.location_tmp_compact),
                                                       shell=True, stdin=subprocess.PIPE)
            delete_tmp_file = subprocess.check_call('hdfs dfs -rm -r {}'.format(self.location_tmp_file), shell=True,
                                                    stdin=subprocess.PIPE)
            #print("********FINE REMOVE_TMP***********")
        except(Exception) as e:
            print("********ERRORE REMOVE_TMP***********")
            raise

            # def __del__(self):

    #    self.sparkUtility.close()

    def runCompattamento(self):
        try:
            #print("********INIZIO COMPATTAMENTO***********")

            # Creazione delle tmp e controllo sull'esistenza di file nelle tmp
            self.hdfs.hdfsMkdir(self.location_tmp_file)
            self.hdfs.hdfsMkdir(self.location_tmp_compact)
            self.check()

            # Operazioni relative al compattamento
            self.compattamentoFile(self.FILE_FORMAT_PARQUET)
            self.copyFile()

            self.delete_move()
            self.remove_tmp()

            self.sparkUtility.close() if self.flag_spark else None
            #print("********FINE COMPATTAMENTO***********")
        except Exception as e:
            print("********ERRORE COMPATTAMENTO***********")
            self.sparkUtility.close() if self.flag_spark else None
            traceback.print_exc()
            raise


# new version
class Compattatore:
    def __init__(self, table, spark=None, hdfs=None):
        self.table = table

        self.hdfs = hdfs if hdfs is not None else Hdfs()
        if spark is None:
            self.sparkUtility = Spark("compattatore", {"hive.exec.max.dynamic.partition.pernode": "10000"})
            self.flag_spark = True
        else:
            self.sparkUtility = spark
            self.flag_spark = False
        self.SQLContext = self.sparkUtility.getSQLContext()

    def changeName(self, name):
        split_name = name.split('_')
        if len(split_name) > 1:
            if split_name[-1] == "compact":
                return '_'.join(split_name[:-1])
        return name + "_compact"

    def getTableInfo(self, table):
        partitions = []
        table_location = None
        flag_partition = False

        res = self.spark.getHiveContext().sql("DESCRIBE FORMATTED {}".format(table)).collect()
        for row in res:
            if row.col_name == "# Partition Information":
                flag_partition = True
            elif flag_partition:
                if row.col_name == '':
                    flag_partition = False
                    continue
                partitions.append(row.col_name)
            elif row.col_name == "Location":
                table_location = row.data_type

        return {"location": table_location, "partitions": partitions[1:]}

    def execute(self):
        try:
            print("init")

            # select from original_table
            print("select from source table")
            query = "SELECT * FROM {}".format(self.table)
            df_source = self.spark.getHiveContext().sql(query)

            # write data in tmp
            print("insert overwrite to dest table")
            info = self.getTableInfo(self.table)
            table_location = info["location"]
            partitions = info["partitions"]
            new_table_location = self.changeName(table_location)
            table_tmp = self.changeName(self.table)
            # vedi repartition invece del coalesce
            df_source.coalesce(1).write \
                .option("path", new_table_location) \
                .mode("overwrite") \
                .partitionBy(*partitions) \
                .saveAsTable(table_tmp)

            # drop tabella originale
            print("drop source table")
            self.spark.getHiveContext().sql("DROP TABLE {}".format(self.table))

            # rm file adl
            print("rm files adl source table")
            subprocess.call(["hadoop", "fs", "-rm", "-r", table_location])

            # rename nuova tabella
            print("rename dest table")
            self.spark.getHiveContext().sql("ALTER TABLE {} RENAME TO {}".format(table_tmp, self.table))

            self.sparkUtility.close() if self.flag_spark else None
            print("end")
        except Exception as e:
            print("********ERRORE COMPATTAMENTO***********")
            self.sparkUtility.close() if self.flag_spark else None
            raise


class DbLogger():

    def __init__(self, db, log_table, run_id, script_name, description="", log_path="",
                 app_id="",file_hdfs="",log_table_hdfs=None ):
        self.db = db
        self.log_table = log_table
        #self.log_table_hdfs= log_table_hdfs
        self.timer = MyTimer( id_="db_logger_" + log_table )
        self.run_id = run_id
        self.script_name = script_name
        self.description = description
        self.log_path = log_path
        self.app_id = app_id
        self.log_table_hdfs=log_table_hdfs
        #self.file_hdfs=file_hdfs

    def setLogPath(self, path):
        self.log_path = path

    def setAppId(self, appId):
        self.app_id = appId

    def logStartExecution(self, validity_minutes=None):
        dt_cols = ["START_TIME", "EXPIRATION_DATE"]

        exp_dt = None
        START_TIME_ENV = os.environ.get( 'START_TIME' )
        if START_TIME_ENV is not None:
            print( 'START_TIME = ' + START_TIME_ENV )
            dt = datetime.strptime( START_TIME_ENV, "%Y-%m-%d %H:%M:%S" )
        else:
            dt = datetime.utcnow().replace( microsecond=0 )

        self.start_dt = dt
        self.validity_minutes = validity_minutes
        if validity_minutes is not None:
            exp_dt = self.start_dt + timedelta( minutes=self.validity_minutes )

        dt = datetime.strftime( dt, format="%Y-%m-%d %H:%M:%S" )
        exp_dt = datetime.strftime( exp_dt, format="%Y-%m-%d %H:%M:%S" )
        row = [{"RUN_ID": self.run_id, "SCRIPT": self.script_name, "DESCRIPTION": self.description, "EXIT_CODE": 1,
                "START_TIME": dt, "APP_ID": self.app_id, "LOG_PATH": self.log_path, "EXPIRATION_DATE": exp_dt}]
        #row_table_hdfs=[{"RUN_ID": self.run_id,"FILE_HDFS":self.file_hdfs}]
        
        print("logStartExecution row = {} ".format(row))
        print("logStartExecution dt_cols = {}".format(dt_cols))
        print("logStartExecution self.log_table = {}".format(self.log_table))

        self.db.insertListOfDictsIntoTable( self.log_table, row, dt_cols=dt_cols, commit=True )
        #self.db.insertListOfDictsIntoTable( self.log_table_hdfs, row_table_hdfs, commit=True )
        
        
    def logEndExecution(self, status=0, description=None,filename_list=None):
        # print("___### self.validity_minutes = " + str(self.validity_minutes))
        print("logEndExecution - INIT")
        ex_time = self.timer.timeSinceStart()

        dt = datetime.utcnow().replace( microsecond=0 )
        dt = datetime.strftime( dt, format="%Y-%m-%d %H:%M:%S" )

        query = "UPDATE {} SET ".format( self.log_table )
        query += "END_TIME=to_timestamp('{}', 'yyyy-MM-dd HH24:mi:ss'), ".format( dt )
        query += "EXIT_CODE={}, ".format( status )
        query += "APP_ID='{}', ".format( self.app_id )
        if description is not None:
            query += "DESCRIPTION='{}', ".format( description.replace( "'", "''" ) )

        # if self.validity_minutes is not None:
        #    exp_dt = self.start_dt + timedelta(minutes=self.validity_minutes)
        #    query += "EXPIRATION_DATE=to_timestamp('{}', 'yyyy-MM-dd HH24:mi:ss'), ".format(exp_dt)
        query += "EXECUTION_TIME={:0.3f} ".format( ex_time )
        query += "WHERE RUN_ID='{}'".format( self.run_id )
        # print(query)
        self.db.executeQuery( query, commit=True )
        print(f"filename_list: {filename_list}")
        print( f"log_table_hdfs: {self.log_table_hdfs}" )
        if filename_list and self.log_table_hdfs:
            print( "**** file_list ****" )
            '''
            if not self.db.checkTableExists(self.log_table_hdfs):
                self.createLogTableHdfs()
            print( "**** tabella esistente ****")
            '''
            res=[]
            for n in filename_list:
                res.append((self.run_id,n))
            self.db.executeMany("insert into {} (run_id,file_hdfs) values (?, ?)".format(self.log_table_hdfs),
                                res,commit=True)
        
    def close(self):
        self.db.close()

    def truncateLogTable(self):
        try:
            query = "truncate table {}".format( self.log_table )
            self.db.executeQuery( query )
        except:
            pass

    def dropLogTable(self):
        try:
            query = "drop table {}".format( self.log_table )
            self.db.executeQuery( query )
        except:
            pass

    def createLogTable(self):
        query = "CREATE TABLE {} ".format( self.log_table )
        query += "(RUN_ID VARCHAR2(128 BYTE) NOT NULL, SCRIPT VARCHAR2(64 BYTE) NOT NULL, "
        query += "DESCRIPTION VARCHAR2(512 BYTE), EXECUTION_TIME FLOAT(126), "
        query += "EXIT_CODE NUMBER(*,0), "
        query += "START_TIME DATE, END_TIME DATE)"
        self.db.executeQuery( query )

    def createLogTableHdfs(self):
        #solo per postgres
        query = "CREATE TABLE {} ".format( self.log_table_hdfs )
        query += "(RUN_ID character varying(128) NOT NULL, FILE_HDFS character varying(300) NOT NULL)"
        print("CreateLogTableHdfs:",query)
        self.db.executeQuery( query )

class Db:
    class DatabaseError(Exception):
        def __init__(self, exc):
            super(Db.DatabaseError, self).__init__(exc)

    DB_TYPE_ORACLE = "ORACLE"
    DB_TYPE_POSTGRES = "POSTGRES"

    DATA_TYPE_TIMESTAMP = "timestamp"
    DATA_TYPE_INTEGER = "int"
    DATA_TYPE_STRING = "string"
    DATA_TYPE_FLOAT = "float"
    DATA_TYPE_LIST = "list"
    LIST_SEPARATOR = "||"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    DICT_SCRIPT = "SCRIPT"
    DICT_KEY = "KEY"
    DICT_VALUE = "VALUE"
    DICT_TYPE = "TYPE_VALUE"

    def __init__(self, driver, url, user, password, jar_filepath, schema=None):
        self.url = url
        self.user = user
        self.password = password
        self.driver = driver
        self.connection = None
        self.jar_filepath = jar_filepath
        self.connected = False
        self.db_type = None
        self.db_schema = schema
        self.openConnection()

    def openConnection(self):
        try:
            # self.connection = jaydebeapi.connect("oracle.jdbc.OracleDriver", "jdbc:oracle:thin:@//{}".format(self.url),
            if "jdbc:oracle:" in self.url:
                self.db_type = self.DB_TYPE_ORACLE
                self.connection = jaydebeapi.connect(self.driver, self.url, {'user': self.user, 'password': self.password},
                                                     self.jar_filepath)
            elif "jdbc:postgresql:" in self.url:
                self.db_type = self.DB_TYPE_POSTGRES
                self.connection = jaydebeapi.connect(self.driver, self.url, [self.user, self.password], self.jar_filepath)

            self.connection.jconn.setAutoCommit(False)
            self.connected = True
        except Exception as e:
            raise self.DatabaseError(e)

    def getConnection(self):
        return self.connection

    def close(self):
        if self.connected:
            self.connection.commit()
            self.connection.close()
            self.connected = False

    def resetConnection(self):
        self.connection.close()
        self.openConnection()

    def checkTableExists(self, tablename):
        if "jdbc:postgresql:" in self.url:
            stmt=f"SELECT to_regclass('{tablename}')"
        else:
            return
        dbcur = self.connection.cursor()
        dbcur.execute( stmt )
        print("RETURN QUERY CHECKTABLES EXIST:", dbcur.fetchall())
        print("dbcur.fetchone() is not None: ", dbcur.fetchall() is not None)
        if dbcur.fetchall() is not None:
            print("TABELLA ESISTE")
            dbcur.close()
            return True
        print( "TABELLA non ESISTE" )
        dbcur.close()
        return False

    def executeMany(self, query, tuples, commit=False):

        print("executeMany - query == {}".format(query))
        print("executeMany - tuples == {}".format(tuples))

        if self.connected:
            cursor = self.connection.cursor()
            try:                            
                cursor.executemany(query, tuples)
                if commit:
                    self.connection.commit()
            except Exception as e:
                raise
            finally:
                cursor.close()
        else:
            pass

    def executeQuery(self, query, parameter=None, commit=False, isSelect=False):
        if self.connected:
            cursor = self.connection.cursor()
            try:                
                # Jdbc doesn't support named parameters (:name, ...).
                cursor.execute(query, parameter) if parameter else cursor.execute(query)
                if commit:
                    self.connection.commit()
                if isSelect:
                    res = cursor.fetchall()
                    return res
            except Exception as e:
                raise
            finally:
                cursor.close()
        else:
            pass

    def truncateTable(self, table_name):
        query = "truncate table {}".format(table_name)
        self.executeQuery(query)

    def truncateWithDelete(self, table_name, where_clause="", commit=False):
        query = "DELETE FROM {}".format(table_name)
        if where_clause:
            query += " WHERE {}".format(where_clause)
        self.executeQuery(query, commit=commit)  # ????

    def renameTable(self, old_table_name, new_table_name):
        query = "rename table {} to {}".format(old_table_name, new_table_name)
        self.executeQuery(query)

    def dropTable(self, table_name):
        query = "drop table {}".format(table_name)
        self.executeQuery(query)

    # value_dict = dictionary contenente i valori da modificare e le chiavi sono i nomi delle colonne
    def updateTableWhere(self, table_name, value_dict, where_clause="", commit=False):
        query = "UPDATE {} SET ".format(table_name)
        query += ", ".join("{}='{}'".format(k, v) for k, v in value_dict.items())
        if where_clause:
            query += " where {}".format(where_clause)

        self.executeQuery(query, commit)

    # Da capire
    def insertListOfDictsIntoTable(self, table_name, l, dt_cols=[], commit=False):
        cols = sorted(list(l[0].keys()))

        if dt_cols:
            non_dt_cols = list(set(cols) - set(dt_cols))
        else:
            non_dt_cols = cols

        cols = non_dt_cols + dt_cols

        l = [[dic[col] for col in cols] for dic in l]

        print("insertListOfDictsIntoTable l = {}".format(l))
        self.insertListIntoTable(table_name, l, non_dt_cols, dt_cols)

        if commit:
            self.connection.commit()

    def insertListIntoTable(self, table_name, l, non_dt_cols, dt_cols=[]):
        '''
        The first values of each row should represent non-datetime fields.
        '''
        print("insertListIntoTable l = {}".format(l))
        print("insertListIntoTable dt_cols = {}".format(dt_cols))
        brackets_str = ", ".join(["{}"] * len(non_dt_cols))
        if dt_cols:
            brackets_str += ", "
            brackets_str += ", ".join(["to_timestamp({}, 'yyyy-MM-dd HH24:mi:ss')"] * len(dt_cols))
        n_cols = len(dt_cols) + len(non_dt_cols)
        # brackets_str = brackets_str.format( *[":" + str( i + 1 ) for i in range( 0, n_cols )] )
        brackets_str = brackets_str.format(*["?" for i in range(0, n_cols)])

        cols_str = ", ".join(non_dt_cols + dt_cols)

        query = "INSERT INTO {} ({}) VALUES ({})".format(table_name, cols_str, brackets_str)
        print("insertListIntoTable query = {}".format(query))
        print("insertListIntoTable cols_str = {}".format(cols_str))
        print("insertListIntoTable brackets_str = {}".format(brackets_str))

        self.executeMany(query, l)

    def executeProcedure(self, procname, commit=False, parameters=None):
        if self.connected:
            procname = procname if ("." not in procname) else procname.split(".")[-1]
            final_procname = "{}.{}".format(self.db_schema, procname)

            proc_str = final_procname + "({})".format(",".join(parameters) if parameters else "")
            if self.db_type in (self.DB_TYPE_POSTGRES, self.DB_TYPE_ORACLE):
                procedure_query = "CALL {}".format(proc_str)

            cursor = self.connection.cursor()
            try: 
                cursor.execute(procedure_query)
            except Exception as e:
                self.connection.rollback()
                raise
            else:
                self.connection.commit() if commit else None
            finally:
                cursor.close()
        else:
            pass

    def setKeyValue(self, script=None, key=None, value=None, type=None):
        table_name = "KEY_VALUE"
        if self.db_schema:
            table_name = "{}.{}".format(self.db_schema, table_name)

        try:
            if type == self.DATA_TYPE_TIMESTAMP:
                date_format = self.DATETIME_FORMAT
                value_str = value.strftime(date_format)
            elif type == self.DATA_TYPE_LIST:
                value_str = self.LIST_SEPARATOR.join(str(x) for x in value)
            else:
                value_str = str(value)

            if self.db_type == self.DB_TYPE_ORACLE:
                q = "MERGE INTO {table} d USING (select 1 FROM DUAL) \
                        ON (d.SCRIPT = '{script}' AND d.KEY = '{key}' AND d.TYPE_VALUE = '{type}') \
                        WHEN MATCHED THEN UPDATE SET d.VALUE = '{value}' \
                        WHEN NOT MATCHED THEN INSERT (SCRIPT, KEY, VALUE, TYPE_VALUE) VALUES ('{script}', '{key}', '{value}', '{type}')".format(
                    table=table_name, script=script, key=key, value=value_str, type=type)
            elif self.db_type == self.DB_TYPE_POSTGRES:
                q = "INSERT INTO {table} (SCRIPT, KEY, VALUE, TYPE_VALUE) " \
                    "VALUES('{script}', '{key}', '{value}', '{type}') " \
                    "ON CONFLICT (SCRIPT, KEY) DO UPDATE SET VALUE = excluded.VALUE".format(table=table_name,
                                                                                            script=script, key=key,
                                                                                            value=value_str, type=type)

            self.executeQuery(query=q, commit=True)

        except Exception as e:
            print("set_key_value - ERRORE")
            raise e

    def get_key_value(self, script, key, type=None, orderBy=None):
        table_name = "KEY_VALUE"
        if self.db_schema:
            table_name = "{}.{}".format(self.db_schema, table_name)

        try:
            q = "SELECT SCRIPT,KEY,VALUE,TYPE_VALUE FROM {table} WHERE SCRIPT = '{script}' AND KEY LIKE '{key}'".format(
                table=table_name, script=script, key=key)
            q += " ORDER BY {}".format(orderBy) if orderBy is not None else ""

            res = self.executeQuery(query=q, isSelect=True)
            if len(res) > 0:
                return_values = []

                for record in res:
                    rec_script = record[0]
                    rec_key = record[1]
                    rec_value = record[2]
                    rec_type = record[3]

                    if type == self.DATA_TYPE_TIMESTAMP:
                        date_format = "%Y-%m-%d %H:%M:%S"
                        rec_value = datetime.strptime(rec_value, date_format)

                    if type == self.DATA_TYPE_LIST:
                        rec_value = rec_value.split(self.LIST_SEPARATOR)
                    if type == self.DATA_TYPE_INTEGER:
                        rec_value = int(rec_value)
                    if type == self.DATA_TYPE_FLOAT:
                        rec_value = float(rec_value)

                    return_values.append(
                        {self.DICT_SCRIPT: rec_script, self.DICT_KEY: rec_key, self.DICT_VALUE: rec_value,
                         self.DICT_TYPE: rec_type})

                return return_values
            return None

        except Exception as e:
            print("get_key_value - ERRORE")
            print(e)
            traceback.print_exc()
            raise e

    def delKeyValue(self, script=None, key=None):
        table_name = "KEY_VALUE"
        if self.db_schema:
            table_name = "{}.{}".format(self.db_schema, table_name)

        try:
            param = (script, key)
            q = "DELETE FROM {table} WHERE SCRIPT = '{script}' AND KEY = '{key}'".format(table=table_name,
                                                                                         script=script, key=key)

            self.executeQuery(query=q, commit=True)

        except Exception as e:
            print("del_key_value - ERRORE")
            raise e


class DbCxOracle:
    DATA_TYPE_TIMESTAMP = "timestamp"
    DATA_TYPE_INTEGER = "int"
    DATA_TYPE_STRING = "string"
    DATA_TYPE_FLOAT = "float"
    DATA_TYPE_LIST = "list"
    LIST_SEPARATOR = "||"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    DICT_SCRIPT = "SCRIPT"
    DICT_KEY = "KEY"
    DICT_VALUE = "VALUE"
    DICT_TYPE = "TYPE_VALUE"

    def __init__(self, user, password, host, port, service):
        self.string_connection = "{}/{}@{}:{}/{}".format( user,
                                                          password,
                                                          host,
                                                          port,
                                                          service )
        self.connection = None
        self.cur = None
        self.connected = False
        self.openConnection()

    def openConnection(self):
        self.connection = cx_Oracle.connect( self.string_connection, encoding="UTF-8", nencoding="UTF-8" )
        self.connection.autocommit = False
        self.cur = self.connection.cursor()
        self.connected = True

    def getConnection(self):
        return self.connection, self.cur

    def close(self):
        if self.connected:
            self.connection.commit()
            self.cur.close()
            self.connection.close()
            self.connected = False

    def resetConnection(self):
        self.cur.close()
        self.connection.close()
        self.openConnection()

    def executeMany(self, query, tuples, commit=False, fields=None):
        values = []
        print("executeMany tuples 1 = {}".format(tuples))
        if tuples in ([], None):
            return
        if fields not in (None, "", []):
            values = []
            for r in tuples:
                value = {}
                for i in range( len( r ) ):
                    value[fields[i]] = r[i]
                values.append( value )
            tuples = values
        self.valori = tuples

        print("executeMany tuples 2 = {}".format(tuples))
        print("executeMany query = {}".format(query))


        if self.connected:
            try:
                self.cur.prepare( query )
                self.cur.executemany( None, tuples )

            except Exception as e:
                error, = e.args
                print( 'Error.code =', error.code )
                print( 'Error.message =', error.message )
                print( 'Error.offset =', error.offset )
                print( "Row", self.cur.rowcount, "has error", error.message )
                self.connection.rollback()
                raise
            else:
                self.connection.commit() if commit else None
        else:
            pass

    def executeQuery(self, query, parameter={}, commit=False, isSelect=False, isSelectColumns=False):
        if self.connected:
            try:
                self.cur.prepare( query )
                self.cur.execute( query, parameter )
            except cx_Oracle.DatabaseError as e:
                error, = e.args
                # self.logger.error('Error.code =', error.code)
                # self.logger.error('Error.message =', error.message)
                # self.logger.error('Error.offset =', error.offset)
                # self.logger.error("Row", self.cur.rowcount, "has error", error.message)
                self.connection.rollback()
                raise e
            else:
                self.connection.commit() if commit else None

            
            if isSelectColumns:
                res = self.cur.fetchall()
                columns = [i[0] for i in self.cur.description]
                return res, columns
            elif isSelect:
                res = self.cur.fetchall()
                return res
            
                

        else:
            pass

    def executeProcedure(self, procname, commit=False, parameters=None):
        if self.connected:
            try:
                self.cur.callproc( procname, parameters ) if parameters is not None else self.cur.callproc( procname )
            except cx_Oracle.DatabaseError as e:
                error, = e.args
                print( 'Error.code =', error.code )
                print( 'Error.message =', error.message )
                print( 'Error.offset =', error.offset )
                print( "Row", self.cur.rowcount, "has error", error.message )
                self.connection.rollback()
                raise
            else:
                self.connection.commit() if commit else None
        else:
            pass

    def getQueryResult(self):
        if self.connected:
            res = self.cur.fetchall()
            return res
        else:
            return None

    def truncateTable(self, table_name):
        query = "truncate table {}".format( table_name )
        self.executeQuery( query )

    def truncateWithDelete(self, table_name, where_clause="", commit=False):
        query = "DELETE FROM {}".format( table_name )
        if where_clause:
            query += " WHERE {}".format( where_clause )
        self.executeQuery( query, commit=commit )  # ????

    def renameTable(self, old_table_name, new_table_name):
        query = "rename table {} to {}".format( old_table_name, new_table_name )
        self.executeQuery( query )

    def dropTable(self, table_name):
        query = "drop table {}".format( table_name )
        self.executeQuery( query )

    def checkTableExist(self, table_name, user):
        query = "select count(*) from all_objects where object_type ='TABLE' and object_name = '{}' and owner='{}'".format(
            table_name.upper(), user.upper() )
        self.executeQuery( query )
        return self.getQueryResult()[0][0]

    def insertListOfDictsIntoTable(self, table_name, l, dt_cols=[], commit=False):
        cols = sorted( list( l[0].keys() ) )

        if dt_cols:
            non_dt_cols = list( set( cols ) - set( dt_cols ) )
        else:
            non_dt_cols = cols

        cols = non_dt_cols + dt_cols

        l = [[dic[col] for col in cols] for dic in l]
        self.insertListIntoTable( table_name, l, non_dt_cols, dt_cols )
        if commit:
            self.connection.commit()

    def insertListIntoTable(self, table_name, l, non_dt_cols, dt_cols=[]):
        '''
        The first values of each row should represent non-datetime fields.
        '''
        brackets_str = ", ".join( ["{}"] * len( non_dt_cols ) )
        if dt_cols:
            brackets_str += ", "
            brackets_str += ", ".join( ["to_timestamp({}, 'yyyy-MM-dd HH24:mi:ss')"] * len( dt_cols ) )
        n_cols = len( dt_cols ) + len( non_dt_cols )
        brackets_str = brackets_str.format( *[":" + str( i + 1 ) for i in range( 0, n_cols )] )
        cols_str = ", ".join( non_dt_cols + dt_cols )

        query = "INSERT INTO {} ({}) VALUES ({})".format( table_name, cols_str, brackets_str )
        self.executeMany( query, l )

    def updateTableWhere(self, table_name, value_dict, where_clause="", commit=False):
        query = "UPDATE {} SET ".format( table_name )
        query += ", ".join( "{}='{}'".format( k, v ) for k, v in value_dict.items() )
        if where_clause:
            query += " where {}".format( where_clause )
        self.executeQuery( query, commit=commit )

    def setKeyValue(self, script=None, key=None, value=None, type=None):
        try:
            if type == self.DATA_TYPE_TIMESTAMP:
                date_format = self.DATETIME_FORMAT
                value_str = value.strftime( date_format )
            elif type == self.DATA_TYPE_LIST:
                value_str = self.LIST_SEPARATOR.join( str( x ) for x in value )
            else:
                value_str = str( value )

            param = {"SCRIPT": script, "KEY": key, "VALUE": value_str, "TYPE_VALUE": type}
            q = "MERGE INTO KEY_VALUE d USING (select 1 FROM DUAL) \
                    ON (d.SCRIPT = :SCRIPT AND d.KEY = :KEY AND d.TYPE_VALUE = :TYPE_VALUE) \
                    WHEN MATCHED THEN UPDATE SET d.VALUE = :VALUE \
                    WHEN NOT MATCHED THEN INSERT (SCRIPT, KEY, VALUE, TYPE_VALUE) VALUES (:SCRIPT, :KEY, :VALUE, :TYPE_VALUE)"

            self.executeQuery( query=q, parameter=param, commit=True )

        except Exception as e:
            print( "set_key_value - ERRORE" )
            raise e

    def getKeyValue(self, script=None, key=None, orderBy=None):
        try:
            q = "SELECT SCRIPT,KEY,VALUE,TYPE_VALUE FROM KEY_VALUE WHERE SCRIPT = :1 AND KEY LIKE :2"
            q += " ORDER BY {}".format( orderBy ) if orderBy is not None else ""
            param = (script, '{}'.format( key ))

            self.executeQuery( query=q, parameter=param )
            res = self.getQueryResult()

            if len( res ) > 0:
                return_values = []
                for record in res:
                    obj_key_value = {}
                    obj_key_value[self.DICT_SCRIPT] = record[0]
                    obj_key_value[self.DICT_KEY] = record[1]
                    value = record[2]
                    type_value = record[3]

                    obj_key_value[self.DICT_TYPE] = type_value

                    # decodifico il value in base al tipo
                    if type_value == self.DATA_TYPE_TIMESTAMP:
                        obj_key_value[self.DICT_VALUE] = datetime.strptime( value, self.DATETIME_FORMAT )
                    elif type_value == self.DATA_TYPE_LIST:
                        obj_key_value[self.DICT_VALUE] = value.split( self.LIST_SEPARATOR )
                    elif type_value == self.DATA_TYPE_INTEGER:
                        obj_key_value[self.DICT_VALUE] = int( value )
                    elif type_value == self.DATA_TYPE_INTEGER:
                        obj_key_value[self.DICT_VALUE] = float( value )
                    elif type_value == self.DATA_TYPE_STRING:
                        obj_key_value[self.DICT_VALUE] = value
                    return_values.append( obj_key_value )

                return return_values
            return None

        except Exception as e:
            print( "get_key_value - ERRORE" )
            print( e )
            traceback.print_exc()
            raise e

    def get_key_value(self, script=None, key=None, type=None, orderBy=None):
        try:
            q = "SELECT * FROM KEY_VALUE WHERE SCRIPT = :1 AND KEY LIKE :2"
            q += " ORDER BY {}".format( orderBy ) if orderBy is not None else ""
            param = (script, '{}'.format( key ))

            self.executeQuery( query=q, parameter=param )
            res = self.getQueryResult()

            if len( res ) > 0:
                return_values = []

                for record in res:
                    rec_script = record[0]
                    rec_key = record[1]
                    rec_value = record[2]
                    rec_type = record[3]

                    if type == self.DATA_TYPE_TIMESTAMP:
                        date_format = "%Y-%m-%d %H:%M:%S"
                        rec_value = datetime.strptime( rec_value, date_format )

                    if type == self.DATA_TYPE_LIST:
                        rec_value = rec_value.split( self.LIST_SEPARATOR )
                    if type == self.DATA_TYPE_INTEGER:
                        rec_value = int( rec_value )
                    if type == self.DATA_TYPE_FLOAT:
                        rec_value = float( rec_value )

                    return_values.append(
                        {self.DICT_SCRIPT: rec_script, self.DICT_KEY: rec_key, self.DICT_VALUE: rec_value,
                         self.DICT_TYPE: rec_type} )

                return return_values
            return None

        except Exception as e:
            print( "get_key_value - ERRORE" )
            print( e )
            traceback.print_exc()
            raise e

    def delKeyValue(self, script=None, key=None):
        try:
            param = (script, key)
            q = "DELETE FROM KEY_VALUE WHERE SCRIPT = :1 AND KEY = :2"

            self.executeQuery( query=q, parameter=param, commit=True )

        except Exception as e:
            print( "del_key_value - ERRORE" )
            raise e


class Logger:
    local_log_filename = None
    mylogger = None
    mylogger_handler = None
    enable_print = False

    def __init__(self, class_name, app_name, local_log_file, time_precision="second", enable_debug=True):
        mode = {
            "day": '%Y%m%d',
            "hour": '%Y%m%d%H',
            "minute": '%Y%m%d%H%M',
            "second": '%Y%m%d%H%M%S%f'
        }

        if time_precision not in mode.keys():
            time_precision = "second"

        # logging.basicConfig(filename=LOCAL_LOG_FILE, level=logging.INFO)

        self.local_log_filename = local_log_file
        self.hdfs_date_format = datetime.today().strftime( mode[time_precision] )

        Logger.mylogger = logging.getLogger( class_name )
        Logger.mylogger.setLevel( logging.INFO )
        self.logger_handler = logging.FileHandler( self.local_log_filename )
        self.logger_handler.setLevel( logging.INFO )

        formatter = logging.Formatter( '%(name)s\t- %(levelname)s\t- (%(threadName)-10s)\t- %(message)s' )
        self.logger_handler.setFormatter( formatter )

        Logger.mylogger.addHandler( self.logger_handler )
        self.mylogger_handler = self.logger_handler

        self.enable_debug = enable_debug

        ENABLE_PRINT = os.environ.get( 'ENABLE_PRINT' )
        if ENABLE_PRINT is not None:
            print( 'ENABLE_PRINT = ' + ENABLE_PRINT )
            self.enable_print = True



    def closeLogger(self, logger_handler):
        self.logger_handler.flush()
        self.logger_handler.close()

    def __del__(self):
        self.mylogger_handler.flush()
        self.mylogger_handler.close()

    def getLoggerFileName(self):
        return self.local_log_filename

    def getLogger(self):
        return Logger.mylogger

    def removeLogger(self):
        proc = subprocess.Popen( "rm -f {}".format( self.getLoggerFileName() ), shell=True )
        proc.communicate()

    def info(self, msg, flag_date=True):
        m = msg if not flag_date else "{} - {}".format( datetime.now().strftime( "%Y/%m/%d %H:%M:%S" ), msg )

        if self.enable_print:
            print( "INFO - {}".format( m ) )
        Logger.mylogger.info( m )

    def warning(self, msg, flag_date=True):
        m = msg if not flag_date else "{} - {}".format( datetime.now().strftime( "%Y/%m/%d %H:%M:%S" ), msg )

        if self.enable_print:
            print( "WARNING - {}".format( m ) )
        Logger.mylogger.warning( m )

    def error(self, msg, flag_date=True, exc_info=None):
        m = msg if not flag_date else "{} - {}".format( datetime.now().strftime( "%Y/%m/%d %H:%M:%S" ), msg )

        if self.enable_print:
            print( "ERROR - {}".format( m ) )

        if exc_info is None:
            Logger.mylogger.error( m )
        else:
            Logger.mylogger.error( m, exc_info )

    def debug(self, msg, flag_date=False):
        caller = inspect.stack()[1][3]
        method = inspect.stack()[2][3] if caller == "wrapper" else caller
        m = msg if not flag_date else "{} - {}".format( datetime.now().strftime( "%Y/%m/%d %H:%M:%S" ), msg )

        if self.enable_debug:
            print( "DEBUG - {} - {}".format(method, m))

    def debug_old(self, msg, flag_date=True):
        m = msg if not flag_date else "{} - {}".format( datetime.now().strftime( "%Y/%m/%d %H:%M:%S" ), msg )

        if self.enable_print:
            print( "DEBUG - {}".format( m ) )
        Logger.mylogger.debug( m )


class Hdfs:
    def __init__(self, webhdfs_url, username):
        try:
            self.client = InsecureClient(url=webhdfs_url, root="/", user=username)
        except Exception as e:
            raise Exception("ERROR Hdfs init: unable to initialize client", e)

    def hdfsLs(self, path):
        return self.client.list(hdfs_path=path, status=False)

    def hdfsWrite(self, fpath, data, append=False, overwrite=False):
        if isinstance(data, str):
            data=data.encode('utf-8')
        elif isinstance(data, dict):
            data = json.dumps(data)
            #print("tipo data: {}".format(type(data)))
            
        print("hdfsWrite - fpath: {}".format(fpath))
        print("hdfsWrite - append: {}".format(append))
        print("hdfsWrite - data: {}".format(data))
        print("hdfsWrite - overwrite: {}".format(overwrite))
                    
        self.client.write(hdfs_path=fpath, data=data, overwrite=overwrite, append=append)

    def hdfsRead(self, fpath, text=True):
        content = None
        
        with self.client.read(hdfs_path=fpath, encoding="UTF-8") as reader:
            content = reader.read() if text else reader.read()
            
        return content
    
    def hdfsGet(self, fpath, lpath, overwrite=False,n_threads=1,temp_dir=None):
        self.client.download(fpath, lpath, overwrite=overwrite, n_threads=n_threads,temp_dir=temp_dir)
    
    
    def hdfsMkdir(self, path):
        # se esiste già, non fa nulla
        self.client.makedirs(path)

    def hdfsMv(self, oldPath, newPath):
        if self.client.status(hdfs_path=newPath, strict=False) is not None:
            self.client.delete(newPath)
            
        self.client.rename(oldPath, newPath)

    def hdfsDelete(self, fpath, recursive=True):
        self.client.delete(hdfs_path=fpath, recursive=recursive)

    def hdfsUpload(self, hdfs_path, local_path):
        self.client.upload(hdfs_path, local_path)

    def hdfsUploadAndRemove(self, hdfs_path, local_path):
        if os.path.isfile(local_path):
            self.client.upload(hdfs_path, local_path)
            os.remove(local_path)
    
    def hdfsGet(self, fpath, lpath, overwrite=False,n_threads=1,temp_dir=None):
        self.client.download(fpath, lpath, overwrite=overwrite, n_threads=n_threads,temp_dir=temp_dir)    


class HdfsSubprocess:

    # (hdfs_put)
    def hdfsPut(self, fpath, local_path, append=True):
        proc = subprocess.Popen( ["hdfs", "dfs", "-put", "-f", local_path, fpath], stdin=subprocess.PIPE )
        print( "retcode =", proc.returncode )
        proc.communicate()

        # metodo per la lettura di un file su hdfs (hdfs_read)

    def hdfsRead(self, fpath, text=True):
        try:
            cmd = '-text' if text else '-cat'
            proc = subprocess.Popen( ['hadoop', 'fs', cmd, fpath], stdout=subprocess.PIPE )
            value = '';
            for line in proc.stdout:
                value += line.decode( "utf-8" )
            return value
        except ValueError as  e:
            return None

        # metodo per la put di un file su hdfs
        # fpath consiste nel path hdfs e il nome del file con estensione,
        # data = la stringa da salvare nel file (hdfs_write)

    def hdfsWrite(self, fpath, data, append=True, type=None):
        proc = subprocess.Popen( ["hdfs", "dfs", "-put", "-f", "-", fpath], stdin=subprocess.PIPE )
        proc.communicate(data)
        #else :
        #    proc.communicate(data.encode( 'utf-8' ) ) if isinstance(data, bytes) else proc.communicate( json.dumps(data, indent=2).encode( 'utf-8' ) )

        # metodo per la cancellazione del contenuto della cartella su hdfs
        # fpath consiste nel path hdfs e il nome del file con estensione,
        # data = la stringa da salvare nel file (hdfs_delete)


    def hdfsMv(self, oldPath, newPath, append=True):
        proc = subprocess.Popen( ["hdfs", "dfs", "-mv", oldPath, newPath], stdin=subprocess.PIPE )
        proc.communicate()

        # metodo per lo spostamento o rename di un file
        # oldPath vecchio nome cartella/file
        # newPath nuovo nome cartella/file


    def hdfsDelete(self, fpath, data, append=False):
        proc = subprocess.Popen( ["hdfs", "dfs", "-rm", "-r", fpath], stdin=subprocess.PIPE )
        proc.communicate( data.encode( 'utf-8' ) )

        # metodo per la lista del contenuto della cartella su hdfs
        # fpath consiste nel path hdfs e il nome del file con estensione,
        # data = la stringa da salvare nel file (hdfs_ls)

    def hdfsLs(self, fpath, data, append=False):
        proc = subprocess.Popen( ["hdfs", "dfs", "-ls", fpath], stdin=subprocess.PIPE )
        proc.communicate( data.encode( 'utf-8' ) )
        if (proc):
            return True
        else:
            return False

        # metodo per la creazione della cartella su hdfs (hdfs_mkdir)

    def hdfsMkdir(self, path):
        proc = subprocess.Popen( "hdfs dfs -mkdir -p {}".format( path ), shell=True )
        proc.communicate()

        # metodo per copiare i file di log in hdfs
        # log_path = il path in cui memorizzare i file di log
        # filename = il nome del file

    def copyLogHdfs(self, hdfs_log_path, local_path_filename):
        print( "hdfs dfs -copyFromLocal  -f  local_path_filename:{} hdfs_log_path:{}".format( local_path_filename,
                                                                                              hdfs_log_path ) )
        proc = subprocess.Popen( "hdfs dfs -copyFromLocal  -f  {} {}".format( local_path_filename, hdfs_log_path ),
                                 shell=True )
        proc.communicate()

    '''def copyLogHdfs(self,log_path,log_filename):  
        #print("hdfs dfs -put -f  {} {}".format(log_filename,log_path))
        #proc = subprocess.Popen("hdfs dfs -put -f  {} {}".format(log_filename,log_path),shell=True)
        #proc.communicate()'''

    # metodo per memorizzare dati su hdfs
    # json_data = la stringa json che si vuole salvare
    # hdfs folder = il path

    def sendToHdfs(self, json_data, hdfs_folder, identificativo_file, flag_return=True):
        json_string = str( json_data )
        # json_string=json.dumps(json_data)
        print( "FOLDER CREATE" )
        d = datetime.today().strftime( '%Y%m%d%H%M%S%f' )
        d1 = datetime.today().strftime( '%Y/%m/%d/%H' )
        folder_hdfs_date = hdfs_folder + d1 + "/"
        print( folder_hdfs_date )
        proc = subprocess.Popen( 'hadoop fs -mkdir "{0}"'.format( folder_hdfs_date ), shell=True )
        proc.communicate()
        print( "FOLDER CREATE" )
        self.hdfsWrite( folder_hdfs_date + identificativo_file + d + "_r.json", json_string )
        if flag_return:
            return folder_hdfs_date + identificativo_file + d + "_r.json"

    def moveFileHdfs(self, file_origin, file_dest=None):
        file_dest = file_origin.replace( "_r", "" )
        proc = subprocess.Popen( 'hadoop fs -mv "{}" "{}"'.format( file_origin, file_dest ), shell=True )
        proc.communicate()

        # (hdfs_write_json)

    def hdfsWriteJson(self, json_data, folder_name, file_name):
        if (isinstance( json_data, list )):
            json_string = str( json_data )
        else:
            json_string = json_data
        proc = subprocess.Popen( 'hadoop fs -mkdir "{0}"'.format( folder_name ), shell=True )
        proc.communicate()
        print( "FOLDER CREATE" )
        full_name = folder_name + file_name
        print( full_name )
        self.hdfsWrite( full_name, json_string )
        return full_name

        # (hdfs_merge_files)

    def hdfsMergeFiles(self, hdfs_folder_name, logFile):
        try:
            cmd = '-text'
            proc = subprocess.Popen(
                'hadoop fs -text {}*.log | hadoop fs -put - {}{}.log'.format( hdfs_folder_name, hdfs_folder_name,
                                                                              logFile ), shell=True )
            proc.communicate()
            # poi fare la hdfs fs copyFromLocal
        except ValueError as  e:
            return None


class Spark:
    spark = None
    sparkContext = None
    hiveContext = None

    def __init__(self, APP_NAME, configDict):
        conf = SparkConf().setAppName( APP_NAME )
        spark_cfg = SparkSession.builder.appName( APP_NAME ).config( conf=conf )

        if configDict:
            for key, value in configDict.items():
                spark_cfg = spark_cfg.config( key, value )

        self.spark = spark_cfg.enableHiveSupport().getOrCreate()
        self.sparkContext = self.spark.sparkContext

    def getSpark(self):
        return self.spark

    def getSparkContext(self):
        return self.sparkContext

    def getApplicationID(self):
        return self.sparkContext.applicationId

    def getSQLContext(self):
        return SQLContext( self.sparkContext )

    def getHiveContext(self):
        if self.hiveContext is None:
            return HiveContext( self.sparkContext )
        else:
            return self.hiveContext
        
    def close(self):
        self.sparkContext.stop()
        
    #def __del__(self):
    #    self.sparkContext.stop()

    def getDfWithKey(self, df, hive_key_list):
        getattr_list = [getattr( df, x ) for x in hive_key_list]
        df = df.withColumn(
            "key",
            sha2(
                concat_ws(
                    "||",
                    *getattr_list
                    # getattr(df,*data)

                ),
                256 )
        )
        return df


    def getDfFromCsv(self, data, header=False):
        #.option("mode", "DROPMALFORMED") \

        return self.spark.read \
            .format("csv") \
            .option("header", header) \
            .load(data)


    #Modifica Michele
    def getDfFromQuery(self, query, oracle_param=None):
        '''
           oracle_param di esempio 
           oracle_param={"url":"127.0.0.1:1521", "user":"utente_login", "password":"utente_password"}
           per oracle query puo' essere o la tabella o la query con l'alias finale esempio:
                 (select * from te_viaggi where ROWNUM <2) te_viaggi
            e senza campi geometrici nella select 
            oracle.jdbc.driver.OracleDriver
            .option("driver", "oracle.jdbc.driver.OracleDriver") \'
            .option("driver", "org.postgresql.Driver") \'
        '''

        if oracle_param is not None:
            print(repr(oracle_param))
            return self.spark.read \
            .format("jdbc") \
            .option("url",oracle_param["url"] ) \
            .option("dbtable", query) \
            .option("user", oracle_param["user"]) \
            .option("password", oracle_param["password"]) \
            .load()
        else:
            #return self.getHiveContext().sql( query )
            return self.getSQLContext().sql( query )


    #Modifica Michele
    def getDfFromOracle(self, query, oracle_param=None):
        

        if oracle_param is not None:
            print(repr(oracle_param))
            return self.spark.read \
            .format("jdbc") \
            .option("url",oracle_param["url"] ) \
            .option("dbtable", query) \
            .option("user", oracle_param["user"]) \
            .option("password", oracle_param["password"]) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .load()
        else:
            #return self.getHiveContext().sql( query )
            return self.getSQLContext().sql( query )
        
        
    

    def getDfLowerColumn(self, df: DataFrame):
        # return  (df.withColumnRenamed(c, c.lower()) for c in df.columns)
        df_lower = df.toDF( *[c.lower() for c in df.columns] )
        return df_lower

    def getDfUpperColumn(self, df: DataFrame):
        # return  (df.withColumnRenamed(c, c.lower()) for c in df.columns)
        df_upper = df.toDF( *[c.upper() for c in df.columns] )
        return df_upper


class Kafka():
    CONSUMER_INSTANCE_NOT_FOUND = 40403

    def __init__(self, topicName, consumerName, consumerInstance):
        self.consumerName = consumerName
        self.consumerInstance = consumerInstance
        self.topicName = topicName
        self.headers = {'Accept': 'application/vnd.kafka.json.v2+json',
                        'Content-Type': 'application/vnd.kafka.json.v2+json'}
        self.initParam()

    def initParam(self):
        self.username = "admin"
        self.password = "anasDSS12345!"

    def createConsumer(self):
        consumer_creation = "https://130.162.113.13:1080/restproxy/consumers/{}".format( self.consumerName )
        payload = {"name": self.consumerName, "format": "json", "auto.offset.reset": "earliest"}
        resp = requests.post( consumer_creation, data=json.dumps( payload ), headers=self.headers, verify=False,
                              auth=(self.username, self.password) )
        resp_dict = json.loads( resp.text )

        if (resp.status_code == 200):
            regex = "/consumers/(.*?)/"
            self.consumerInstance = str( re.findall( regex, resp_dict["base_uri"] )[0] )
            self.consumerName = str( resp_dict["instance_id"] )
        return self.consumerName, self.consumerInstance

    def subscribe(self):
        consumer_subscription = "https://130.162.113.13:1080/restproxy/consumers/{}/instances/{}/subscription".format(
            self.consumerInstance, self.consumerName )
        payload = {"topics": [self.topicName]}
        print( "Kafka - subscribe - consumer_subscription == " + consumer_subscription )
        print( "Kafka - subscribe - self.topicName == " + self.topicName )

        resp = requests.post( consumer_subscription, data=json.dumps( payload ), headers=self.headers, verify=False,
                              auth=(self.username, self.password) )
        print( "Kafka - subscribe - resp.status_code = " + str( resp.status_code ) )

        if (resp.status_code < 200 or resp.status_code > 299):
            print( "Kafka - subscribe - subscribe error" )

    def deleteConsumer(self):
        consumer_delete = "https://130.162.113.13:1080//restproxy/consumers/{}/instances/{}".format(
            self.consumerInstance, self.consumerName )
        payload = {"consumerInstanceId": self.consumerInstance}
        resp = requests.delete( consumer_delete, data=json.dumps( payload ), headers=self.headers, verify=False,
                                auth=(self.username, self.password) )
        if (resp.status_code != 200):
            print( "deleteConsumer Error" )

    def readTopic(self):
        print( "Kafka - readTopic - init" )
        topic_url = "https://130.162.113.13:1080/restproxy/consumers/{}/instances/{}/records/".format(
            self.consumerInstance, self.consumerName )
        print( "Kafka - readTopic - topic_url = " + topic_url )
        resp = requests.get( topic_url, headers=self.headers, verify=False, auth=(self.username, self.password) )

        print( "Kafka - readTopic - resp.status_code = " + str( resp.status_code ) )
        if (resp.status_code == 200):
            return json.loads( resp.text )
        elif (resp.status_code == 404):
            print( "Kafka - readTopic - Consumer instance not found." )
            raise ErrorException( None, self.CONSUMER_INSTANCE_NOT_FOUND )
            # raise Exception("CONSUMER_INSTANCE_NOT_FOUND")
            # Common.raiseException(None, None, self.CONSUMER_INSTANCE_NOT_FOUND)
        elif (resp.status_code == 406):
            print(
                "Kafka - readTopic - Consumer format does not match the embedded format requested by the Accept header." )
            raise Exception( "Consumer format does not match the embedded format requested by the Accept header." )
        elif (resp.status_code == 500):
            print( "Kafka - readTopic - Error 500" )
            data_error = json.loads( resp.text )
            print( "Kafka - readTopic - Consumer problem in read Topics." + data_error['error_code'] )
            self.deleteConsumer()
            raise Exception( "Consumer problem in read Topics." )


'''
  def getConsumerOffset(self,topic_url):
      #/consumers/(string: group_name)/instances/(string: instance)/offsets
       ctype=config.get('KAFKA_CONSUMER','CONTENT_TYPE')
       headers = {'Accept' : ctype} 
       consumer=config.get('KAFKA_CONSUMER','C_CONSUMER')
       instance=config.get('KAFKA_CONSUMER','C_INSTANCE')
                    
       topic_url=topic_url+"{}/instances/{}/offsets/".format(instance,str(consumer))
       print("topic_url {}".format(topic_url))
       resp = requests.get(topic_url, headers=headers,verify=False,auth=(self.username, self.password))
       print(resp.text)
       if (resp.status_code == 200):
          print("Consumer instance Offset ")
          # convert 'str' to Json
          data = json.loads(resp.text)

          self.consumerImp.runs(data)
       if (resp.status_code == 404):
          print("Consumer instance or Partition not found.")
          print(instance,consumer,ctype)
          raise Exception("Consumer instance not found.")
          
       if (resp.status_code==406):
          print("Consumer format does not match the embedded format requested by the Accept header." )
          raise Exception("Consumer format does not match the embedded format requested by the Accept header.")
       if (resp.status_code==500):
          print("Consumer problem in read Topics." )
          raise Exception("Consumer problem in read Topics.")'''


class Common:

    def union_date_intervals(self, intervals):
        """
            funzione che restituisce l'unione di intervalli di date, solo se tali intervalli si sovrappongono (estremi esclusi). Se non c'è
            sovrapposizione, verranno restituiti gli stessi intervalli di input
        :param intervals (list): lista intervalli di date da processare (ogni elemento è composto da due date 'DA','A')
        :return: return_intervals (list): lista intervalli uniti, o lista vuota se l'input non è valido

        """

        if intervals is None or len( intervals ) == 0:
            return []

        # creo un array di tuple ("DA", data), ("A", data) contenente tutte le date realtive agli intervalli in input
        return_intervals = []
        merge_date = []
        for inter in intervals:
            tuple_da = ("DA", inter[0])
            tuple_a = ("A", inter[1])
            merge_date.append( tuple_da )
            merge_date.append( tuple_a )

        # ordino per data la lista appena calcolata
        # sorted(sorted(a, key=lambda x: x[0]), key=lambda x: x[1], reverse=True)
        merge_date.sort( key=(lambda x: x[1]) )
        print( merge_date )

        # costruisco gli intervalli finali (con eventuali unioni)
        union_da = None
        flag_balance = 0
        for tuple in merge_date:
            if union_da is None:
                union_da = tuple[1]
                flag_balance += 1
            elif tuple[0] == "DA":
                flag_balance += 1
            else:  # elem[0] == "A"
                flag_balance -= 1
                if flag_balance == 0:
                    union_a = tuple[1]
                    return_intervals.append( [union_da, union_a] )
                    union_da = None
        return return_intervals

    def getNumExecution(self, date: datetime, validity_minutes: int):
        '''
        calcola il numero di esecuzioni a partire dalla mezzanotte

        date: data relativa all'esecuzione
        validity_minutes: durata singola esecuzione
        :return: int
        '''
        seconds_since_midnight = int(
            (date - date.replace( hour=0, minute=0, second=0, microsecond=0 )).total_seconds() )
        minutes_since_midnight = int( seconds_since_midnight / 60 )
        return int( minutes_since_midnight / validity_minutes )

    # Funzione per l'arrotondamento dei minuti di una data
    def roundDate(self, date, minutes_to_round=10):
        if int( minutes_to_round ) == 10:
            roud_min = date.minute - date.minute % 10
            return date.replace( minute=roud_min, second=0, microsecond=0 )
        elif int( minutes_to_round ) == 5:
            roud_min = date.minute - date.minute % 5
            return date.replace( minute=roud_min, second=0, microsecond=0 )
        return None

    # Se obj non è di tipo list ritorna un array di 1 elemento che include obj
    def toList(self, obj):
        if obj is None :
            return []

        retList = obj
        if not isinstance( retList, list ):
            retList = [retList]
        else:
            retList2 = []
            for item in retList:
                if isinstance( item, list ):
                    retList2.extend( item )
                else:
                    retList2.append( item )

            return retList2

        return retList

    def buildHiveQuery(self, schema, table, filters=None, option=None, fields=None):
        query_fields = ",".join( fields ) if fields is not None else "*"
        query = "SELECT {} FROM {}.{}".format( query_fields, schema,
                                               table )
        keys = filters.keys() if filters is not None else {}
        flag_where = True
        for k in keys:
            if flag_where:
                query += " where {} = {}".format( k, filters[k] )
                flag_where = False
            else:
                query += " and {} = {}".format( k, filters[k] )

        query = query + option if option is not None else query
        return query

    def convertPbToJson(self,pb):
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(pb)
        return json_format.MessageToJson(feed)


    def sendMail(self):
        recipients = "@almaviva.it"
        message = "Subject: Servizio RMT_PUSH in errore\n"
        p = subprocess.Popen( ['mail', '-s', message, recipients], stdout=subprocess.PIPE )
        p.communicate()

        # La funzione initializeVar puo essere utilizzata per istanziare le variabili contenute nel file config.ini, rispettando il seguente protocollo:
        # 1. Nell'ini tutti i nomi delle variabili devono essere scritti in lower-case
        # 2. I dictionary devono essere rappresentati tra parentesi graffe
        # 3. Le liste devono essere rappresentate tra parentesi quadre, separando con la virgola gli elementi
        # 4. Tutte le altre variabili saranno trattate come stringhe
        # E restituisce self.nomevariabile=valore

    # mod Giuseppe
    def initializeVar(self, class_, config, section=None):
        from pyspark.sql.types import StructType, StructField
        if section is not None:
            sections = section
        else:
            sections = config.sections()
        for section in sections:
            for item in config.items( section ):
                name = item[0]
                value = item[1]
                print( name, ": ", value )
                if value != "":
                    if value.lower() == "true":
                        value = True
                    elif value.lower() == "false":
                        value = False
                    elif value[0] == "[" and value[-1] == "]":
                        value = ast.literal_eval( value )
                    elif value[0] == "{" and value[-1] == "}":
                        value = ast.literal_eval( value )
                    elif value.startswith( "StructType" ):
                        value = eval( value )
                setattr( class_, name, value )

    '''
    def initializeVar(self,class_,config,section=None):
        from pyspark.sql.types import StructType,StructField

        if section is not None:
            sections=section
        else:
            sections=config.sections()

        for section in sections:
            for item in config.items(section):
                name = item[0]
                value = item[1]
                execString = "class_.{}= '{}'".format(name,value)
                if value != "" :
                  if value.lower()=="true":
                      value = True
                      execString = "class_.{}= {}".format(name,value)
                  elif value.lower()=="false":
                      value = False
                      execString = "class_.{}= {}".format(name,value)
                  elif value[0]=="[" and value[-1]=="]":
                      execString = "class_.{}= {}".format(name,value)
                  elif value[0]=="{" and value[-1]=="}":
                      value = ast.literal_eval(value)
                      execString = "class_.{}= {}".format(name,value)
                  elif value.startswith("StructType"):
                      print("***importo StructType e StructField")
                      execString = "class_.{}= {}".format(name,value)

                print(execString)
                exec(execString)

    '''
    '''def initializeVarOld(self,item):
        name = item[0]
        value = item[1]
        execString = "self.{}= '{}'".format(name,value)  
        if name[0]=="[" and name[-1]=="]":
            execString = "self.{}= {}".format(name,value)
        elif name[0]=="{" and name[-1]="}":
            value = ast.literal_eval(value)
            execString = "self.{}= {}".format(name,value)
        return execString'''

    # metodo per il controllo se la stringa  un json valido (is_json)
    def isJson(self, myjson):
        try:
            json_object = json.dumps( myjson )
        except ValueError as  e:
            return False
        return True

    #ritorna None se key non esiste, altrimenti restituisce il valore
    def getJsonValue(self, data, *args, defval=None):
        try:
            val = data
            for x in args :
               val = val[x]
            return val
        except:
            return defval

    # ritorna true se la chiave è presente almeno una volta nel json (per json innestati)
    def checkJsonKeyExists(self, json: dict, key: str):
        if key in json.keys():
            return True

        for elem in json.values():
            if isinstance(elem, dict):
                if self.checkJsonKeyExists(elem, key):
                    return True
            elif isinstance(elem, list) or isinstance(elem, tuple):
                for list_item in elem:
                    if isinstance(list_item, dict):
                        if self.checkJsonKeyExists(list_item, key):
                            return True
        return False

    def merge_dicts(self, dict1, dict2):
        """ merge ricorsivo di due dict, sovrascrive i valori in 'conflitto' """
        if not isinstance(dict1, dict) or not isinstance(dict2, dict):
            return dict2

        for k in dict2:
            if k in dict1:
                dict1[k] = self.merge_dicts(dict1[k], dict2[k])
            else:
                dict1[k] = dict2[k]

        return dict1

    # panama_traffico_veicolare_%Y%m%d%H%M%S%f.csv
    def getStringTodayFormat(self, stringTime):
        today = datetime.today()
        return stringTime \
            .replace( '%Y', str( today.year ) ) \
            .replace( '%m', "{:02d}".format( today.month ) ) \
            .replace( '%d', "{:02d}".format( today.day ) ) \
            .replace( '%H', "{:02d}".format( today.hour ) ) \
            .replace( '%M', "{:02d}".format( today.minute ) ) \
            .replace( '%S', "{:02d}".format( today.second ) ) \
            .replace( '%f', str( today.microsecond ) )

        # (get_file_name_time)

    def getFileNameTime(self, extension, prefix=None, suffix=None):
        file_name_time = ""
        if (prefix):
            file_name_time = prefix + "_"
        file_name_time = file_name_time + datetime.today().strftime( '%Y%m%d%H%M%S%f' )
        if (suffix):
            file_name_time = file_name_time + "_" + suffix
        return file_name_time + "." + extension

        # (get_folder_name_time)

    def getFolderNameTime(self, hdfs_folder):
        return hdfs_folder + datetime.today().strftime( '%Y/%m/%d/%H' ) + "/"

    # modifica lo schema passato convertendo StructField in minuscolo per hive
    def lowerStructField(self, schema):
        for s in schema:
            s.name = s.name.lower()
            if type( s.dataType ) == ArrayType:
                self.lowerStructField( s.dataType.elementType )
        return schema

    def set_df_columns_nullable(self, spark, df, column, nullable=True):
        for struct_field in df.schema:
            if struct_field.name == column:
                struct_field.nullable = nullable
        df_mod = spark.createDataFrame( df.rdd, df.schema )
        return df_mod

    def nullValuesManager(self, value):
        try:
            to_ret = None
            if(value):
                to_ret = value
            return to_ret
        except KeyError as e:
            return None

    def generateIdByParameter(self, parameter, prefix = None, suffix = None):
        id = None
        if (parameter == "ALPHANUMERIC"):
            id = str(uuid.uuid4())
        if(parameter == "TIME"):
            id = self.generateId(prefix = prefix, suffix = suffix)

        return id

    def generateId(self, prefix = None, suffix = None):
        id = ""
        if (prefix):
            id = prefix + "_"
        id = id + datetime.today().strftime( '%Y%m%d%H%M%S%f' )
        if (suffix):
            id = id + "_" + suffix
        return id

    def getExceptionCode(self, e, lv2=00):
        error_code = ErrorException.GENERIC_ERROR
        if isinstance( e, ErrorException ):
            error_code = e.code
        return (error_code * 100) + lv2

    # Metodo che costruisce e restituisce un Errorxception partendo dalla eccezione
    # Se è già ErrorExcetion la ritorna senza eseguire operazioni
    def getException(self, e, key):

        if isinstance( e, ErrorException ):
            return e

        type_ = str( type( e ).__name__ )
        if "." in type_:
            exceptionName = type_.split( "." )[:-1]
        else:
            exceptionName = type_

        exceptionList = {
            "SQLExceptionPyRaisable": ErrorException.ORACLE_CONNECTION_ERROR,
            "SQLRecoverableExceptionPyRaisable": ErrorException.ORACLE_CONNECTION_ERROR,
            "RuntimeExceptionPyRaisable": ErrorException.ORACLE_CONNECTION_ERROR,
            "BatchUpdateExceptionPyRaisable": ErrorException.ORACLE_GENERIC_ERROR,
            "DatabaseError": ErrorException.ORACLE_GENERIC_ERROR,
            "AnalysisException": ErrorException.HIVE_WRITE_ERROR,
            "NoOptionError": ErrorException.HIVE_WRITE_ERROR,
            "FileNotFoundError": ErrorException.HDFS_READ_WRITE_ERROR,
            "LookupError": ErrorException.HDFS_READ_ERROR,
            "KeyError": ErrorException.HDFS_READ_ERROR,
            "JSONDecodeError": ErrorException.APPLICATION_GENERIC_ERROR,
            "TypeError": ErrorException.APPLICATION_GENERIC_ERROR,
            "AttributeError": ErrorException.APPLICATION_GENERIC_ERROR,
            "ValueError": ErrorException.APPLICATION_GENERIC_ERROR,
            "OverflowError": ErrorException.APPLICATION_GENERIC_ERROR,
            "Py4JJavaError": ErrorException.APPLICATION_GENERIC_ERROR,
            "IllegalArgumentException": ErrorException.APPLICATION_GENERIC_ERROR,
            "NoSectionError": ErrorException.APPLICATION_GENERIC_ERROR
        }

        exc = ErrorException( e, ErrorException.GENERIC_ERROR )

        if exceptionName in exceptionList:
            exc = ErrorException( e, exceptionList[exceptionName] )
        elif key == ErrorException.KEY_HIVE:
            exc = ErrorException( e, ErrorException.HIVE_GENERIC_ERROR )
        elif key == ErrorException.KEY_ORACLE:
            exc = ErrorException( e, ErrorException.ORACLE_GENERIC_ERROR )
        elif key == ErrorException.KEY_APPLICATION:
            exc = ErrorException( e, ErrorException.APPLICATION_GENERIC_ERROR )

        return exc

    def raiseException(self, e, key):
        type_ = str( type( e ) )

        Exception_ = "<class 'Exception'>"
        SQLExceptionPyRaisable_ = "<class 'jpype._jexception.java.sql.SQLExceptionPyRaisable'>"  # accesso ad oracle fallito (user/pass)
        SQLRecoverableExceptionPyRaisable_ = "<class 'jpype._jexception.java.sql.SQLRecoverableExceptionPyRaisable'>"  # accesso oracle fallito (porta errata)
        RuntimeExceptionPyRaisable_ = "<class 'jpype._jexception.java.lang.RuntimeExceptionPyRaisable'>"  # accesso oracle fallito (jar path non valido)
        BatchUpdateExceptionPyRaisable_ = "<class 'jpype._jexception.java.sql.BatchUpdateExceptionPyRaisable'>"  # log_table non valida (db_logger)
        JSONDecodeError_ = "<class 'json.decoder.JSONDecodeError'>"  # url invalido post (callservice)
        KeyError_ = "<class 'KeyError'>"  # popen fpath non valido (hdfs_read)
        NameError_ = "<class 'NameError'>"  # type non valido (callservice)
        FileNotFoundError_ = "<class 'FileNotFoundError'>"  # Popen (hdfs_read e hdfs_write)
        TypeError_ = "<class 'TypeError'>"  # createDataFrame row errato (spark), campo json invlido post (callservice)
        OverflowError_ = "<class 'OverflowError'>"  # date value incorrect (db_logger)
        LookupError_ = "<class 'LookupError'>"  # argomento decode invalido (hdfs_read)
        AttributeError_ = "<class 'AttributeError'>"  # datetime.datetime
        Py4JJavaError_ = "<class 'py4j.protocol.Py4JJavaError'>"  # json schema spark, format erraro nella write(hive)
        IllegalArgumentException_ = "<class 'pyspark.sql.utils.IllegalArgumentException'>"  # mode write errato (hive), mode errato write.jdbc (writeInOracle)
        AnalysisException_ = "<class 'pyspark.sql.utils.AnalysisException'>"  # partitionby write (hive) errata,
        NoOptionError_ = "<class 'configparser.NoOptionError'>"  # path write (hive) invalido,
        NoSectionError_ = "<class 'configparser.NoSectionError'>"  # hdfs_mkdir, saveAsTable (hive)
        ValueError_ = "<class 'ValueError'>"
        AnalysisException_ = "<class 'pyspark.sql.utils.AnalysisException'>"  # database 'data_raw_test' not found

        print( type_ )
        print( key )

        if type_ == SQLExceptionPyRaisable_:
            raise ErrorException( e, ErrorException.ORACLE_CONNECTION_ERROR )
        elif type_ == SQLRecoverableExceptionPyRaisable_:
            raise ErrorException( e, ErrorException.ORACLE_CONNECTION_ERROR )
        elif type_ == RuntimeExceptionPyRaisable_:
            raise ErrorException( e, ErrorException.ORACLE_CONNECTION_ERROR )
        elif type_ == AnalysisException_:
            raise ErrorException( e, ErrorException.ORACLE_GENERIC_ERROR )
        elif type_ == BatchUpdateExceptionPyRaisable_:
            raise ErrorException( e, ErrorException.ORACLE_GENERIC_ERROR )

        elif type_ == AnalysisException_:
            raise ErrorException( e, ErrorException.HIVE_WRITE_ERROR )
        elif type_ == NoOptionError_:
            raise ErrorException( e, ErrorException.HIVE_WRITE_ERROR )

        elif type_ == FileNotFoundError_:
            raise ErrorException( e, ErrorException.HDFS_READ_WRITE_ERROR )
        elif type_ == LookupError_:
            raise ErrorException( e, ErrorException.HDFS_READ_ERROR )
        elif type_ == KeyError_:
            raise ErrorException( e, ErrorException.HDFS_READ_ERROR )

        elif type_ == JSONDecodeError_:
            raise ErrorException( e, ErrorException.APPLICATION_GENERIC_ERROR )
        elif type_ == TypeError_:
            raise ErrorException( e, ErrorException.APPLICATION_GENERIC_ERROR )
        elif type_ == AttributeError_:
            raise ErrorException( e, ErrorException.APPLICATION_GENERIC_ERROR )
        elif type_ == ValueError_:
            raise ErrorException( e, ErrorException.APPLICATION_GENERIC_ERROR )
        elif type_ == OverflowError_:
            raise ErrorException( e, ErrorException.APPLICATION_GENERIC_ERROR )
        elif type_ == Py4JJavaError_:
            raise ErrorException( e, ErrorException.APPLICATION_GENERIC_ERROR )
        elif type_ == IllegalArgumentException_:
            raise ErrorException( e, ErrorException.APPLICATION_GENERIC_ERROR )
        elif type_ == NoSectionError_:
            raise ErrorException( e, ErrorException.APPLICATION_GENERIC_ERROR )

        elif key == ErrorException.KEY_HIVE:
            raise ErrorException( e, ErrorException.HIVE_GENERIC_ERROR )
        elif key == ErrorException.KEY_ORACLE:
            raise ErrorException( e, ErrorException.ORACLE_GENERIC_ERROR )
        elif key == ErrorException.KEY_APPLICATION:
            raise ErrorException( e, ErrorException.APPLICATION_GENERIC_ERROR )

        else:
            raise ErrorException( e, ErrorException.GENERIC_ERROR )

    def createGeometryPointObj(self, typeObj, pointTypeObj, SDO_GTYPE, SDO_SRID, X, Y):
        geom = typeObj.newobject()
        geom.SDO_GTYPE = SDO_GTYPE
        geom.SDO_SRID = SDO_SRID
        geom.SDO_POINT = pointTypeObj.newobject()
        geom.SDO_POINT.X = X
        geom.SDO_POINT.Y = Y

        return geom

    def getMonthYearIter(self, start_month, start_year, end_month, end_year ):
        ym_start= 12*start_year + start_month - 1
        ym_end= 12*end_year + end_month
        for ym in range( ym_start, ym_end ):
            y, m = divmod( ym, 12 )
            yield y, m+1
     

    def buildPartitionQuery(self, date_from,date_to,prefix=""):
        print (date_from)
        print (date_to)
        year_from = date_from.year
        month_from = date_from.month
        day_from = date_from.day

        year_to = date_to.year
        month_to = date_to.month
        day_to = date_to.day

        query = ""

        if year_from == year_to :
           query += "and {}year={} ".format(prefix,year_from)  
           if month_from == month_to :
              query += "and {}month={} ".format(prefix,month_from)  
              if day_from == day_to :
                query += "and {}day={} ".format(prefix,day_from)  
              else :
                q_day = ""
                for count in range(day_from,day_to):
                   q_day += "{}day={} or ".format(prefix,count)
                #query += "and ({}day>={} or {}day<={}) ".format(prefix,day_from,prefix,day_to)  
                query += "and ({}{}day={}) ".format(q_day,prefix,day_to)  
           else :
              q_month = ""
              for count in range(month_from,month_to):
                 q_month += "{}month={} or ".format(prefix,count)
              #query += "and ({}month>={} or {}month=<{})".format(prefix,month_from,prefix,month_to)  
              query += "and ({}{}month={}) ".format(q_month,prefix,month_to)  


        else :
           y_m = self.getMonthYearIter(month_from,year_from,month_to,year_to)
           q_year = "AND ("
           for y,m in y_m :
             q_year += "({}month={} AND {}year={}) OR ".format(prefix,m,prefix,y)  
           query = q_year[:-4] + ")"

        return query
    
    def find_filenames_orderbyKey(self, path_to_dir, suffix=".xlsx",key=os.path.getmtime):
        filenames = glob.glob(path_to_dir+"\\*"+suffix)
        return sorted(filenames,key=key)

class ManageExcelFile:
    def __init__(self,filename):
        self.wb = xlrd.open_workbook( filename )


class SqlAlchemy:
    def __init__(self,base, allClasses, db_url,create=False,delete_all=False,verbose=False, schema=None):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        self.logger = logging.getLogger( __name__ )
        try:
            self.db_url=db_url
            self.base=base
            self.allClasses=allClasses
            self.checkOperation = False
            if schema:
                self.engine = create_engine( self.db_url,
                    connect_args={'options': '-csearch_path={}'.format(schema)},
                    echo=verbose )
            else:
                self.engine = create_engine( self.db_url,
                                         echo=verbose )

            # sessionmaker returns a class
            self.session = sessionmaker( bind=self.engine )()
            self.checkTableExist(create)
            self.deleteAll(delete_all)
            self.checkOperation=True
        except Exception as e:
            self.logger.error("Errore initializeSessionDb ", e)
    
    def getSession(self):
        return self.session
    
    def checkTableExist(self,create):
        # Check if it has the tables
        try:
            for table in self.base.metadata.tables.keys():
                if not self.engine.has_table( table ):
                    if create:
                        self.logger.info( "Creazione tabella {}".format( table ) )
                        self.base.metadata.tables[table].create( self.engine )
                    else:
                        self.logger.info( "Tabella assente {}}!".format( table ) )
                        exit( 1 )
            return True
        except Exception as e:
            self.logger.error("Errore checkTableExist ", e)
            return False

    def deleteAll(self,delete_all):
        #Go through all of the tables that we create, clear them
        if delete_all:
            for theClass in self.allClasses:
                for obj in self.session.query( theClass ):
                    self.session.delete( obj )



#### DECORATORS ####
class TimingAndProfile_old(object):
    def __init__(self, enable_profile=False):
        self.enable_profile = enable_profile

    def __call__(self, func, *args, **kwargs):
        def wrapper(*args, **kwargs):
            m1 = memory_profiler.memory_usage()
            t1 = datetime.now()

            if self.enable_profile:
                to_return = memory_profiler.profile(func)(*args, **kwargs)
            else:
                to_return = func(*args, **kwargs)

            t2 = datetime.now()
            m2 = memory_profiler.memory_usage()
            print("{} - Execution Time: {} seconds".format(func.__name__, t2-t1))
            print("{} - Memory Usage: {} MiB".format(func.__name__, m2[0]-m1[0]))

            return to_return
        return wrapper


class TimingAndProfile(object):
    def __init__(self, enable_profile=False):
        self.enable_profile = enable_profile

    def __call__(self, func=None, *args, **kwargs):
        if func is None:
            return partial(*args, **kwargs)

        @wraps(func)
        def wrapper(*args, **kwargs):
            m1 = memory_profiler.memory_usage()
            t1 = datetime.now()

            if self.enable_profile:
                to_return = memory_profiler.profile(func)(*args, **kwargs)
            else:
                to_return = func(*args, **kwargs)

            t2 = datetime.now()
            m2 = memory_profiler.memory_usage()
            print("DEBUG - TimingAndProfile - [{}] - Execution Time: {} seconds".format(func.__name__, (t2-t1).total_seconds()))
            print("DEBUG - TimingAndProfile - [{}] - Memory Usage: {} MiB".format(func.__name__, m2[0]-m1[0]))

            return to_return
        return wrapper

##############################################################################################
############################     GESTIONE ECCEZIONI INIT     #################################
#############################################################################################


class ErrorException( Exception ):
    KEY_ORACLE = "ORACLE"
    KEY_HIVE = "HIVE"
    KEY_APPLICATION = "APPLICATION"
    KEY_GENERIC = "GENERIC"

    GENERIC_ERROR = 2

    HDFS_GENERIC_ERROR = 10
    HDFS_FILE_NOT_FOUND = 11
    HDFS_CANNOT_WRITE = 12
    HIVE_GENERIC_ERROR = 20
    HIVE_TABLE_NOT_FOUND = 21
    HIVE_CONNECTION_ERROR = 22
    HIVE_REFRESH_TABLE = 23
    HIVE_CANNOT_INSERT = 24
    HIVE_NO_OUTPUT_DATA = 25
    HIVE_NO_INPUT_DATA = 26
    HIVE_CANNOT_READ = 27  # todo da aggiungere a db
    DATABASE_GENERIC_ERROR = 30
    DATABASE_CONNECTION_ERROR = 31
    DATABASE_NO_OUTPUT_DATA = 32
    DATABASE_NO_INPUT_DATA = 33
    SPARK_GENERIC_ERROR = 40
    SPARK_EXCEPTION = 41
    APPLICATION_GENERIC_ERROR = 50
    APPLICATION_EXECUTION_ERROR = 51
    APPLICATION_OUT_OF_MEMORY = 52
    APPLICATION_IMPORT_ERROR = 53
    APPLICATION_TYPE_ERROR = 54
    APPLICATION_RUNTIME_ERROR = 55
    OTHER = 60

    # Todo da eliminare
    ORACLE_GENERIC_ERROR = 30
    ORACLE_CONNECTION_ERROR = 31
    HIVE_WRITE_ERROR = 24
    HDFS_READ_ERROR = 27
    HDFS_READ_WRITE_ERROR = 27

    def __init__(self, exception, code, info=None):
        self.exception = exception
        self.code = code
        self.name = type( exception ).__name__
        self.args = exception.args #None if (exception is None) else exception.args
        self.info = info


'''
class HttpErrorException(ErrorException):
    def __init__(self, exception, code, info=None, response=None):
        super(HttpErrorException, self).__init__(exception, code, info)
        self.response = response
'''

# Aggiunta classe per ingestion file xml e object ANDREA [c'è anche in FlowControl!]
'''
class MyTrasp(Transport):
    res_xml=None

    def post_xml(self,address, envelope, headers):
        res = super(MyTrasp, self).post_xml(address, envelope, headers)
        self.res_xml = res
        return res
'''


class MyTrasp( Transport ):
    res_xml = None

    def __init__(self, cache=None, timeout=300, operation_timeout=None, session=None):
        super( MyTrasp, self ).__init__( cache, timeout, operation_timeout, session )

    def post_xml(self, address, envelope, headers):
        res = super( MyTrasp, self ).post_xml( address, envelope, headers )
        self.res_xml = res
        return res

class FieldControl:
   def checkRequired(self, value, field_name):
       if value is None:
           error_message = "The field {} cannot be null\n".format(field_name)
           raise ErrorException(exception = Exception(error_message), code = ErrorException.GENERIC_ERROR, info = error_message)

   def checkDate(self, value, field_name, fuzzy = False, required = False,date_format="yyyy-MM-dd HH24:mi:ss"):
       try:
           if (required):
               self.checkRequired(value, field_name)
           return None if (value is None) else dateparser(value, fuzzy = fuzzy)
       except ValueError as e:
           error_message = "The field {} is not a date string".format(field_name)
           raise ErrorException(exception = e, code = ErrorException.GENERIC_ERROR, info = error_message)
       except Exception as err:
           raise err

   def checkString(self, value, field_name, fuzzy = False, required = False):
       try:
           if (required):
               self.checkRequired(value, field_name)

           if value is None :
               return None
           elif isinstance(value, str):
               return value
           else:
               error_message = "Required type for field {} is String".format(field_name)
               raise ErrorException(exception = Exception(error_message), code=ErrorException.GENERIC_ERROR, info=error_message)
       except Exception as err:
           raise err

   def checkNumber(self, value, field_name, fuzzy = False, required = False):
       try:
           if (required):
               self.checkRequired(value, field_name)

           if value is None :
               return None
           elif (isinstance(value, float) or isinstance(value, int) or isinstance(value, complex)):
               return value
           else:
               error_message = "Required type for field {} is a number of any type".format(field_name)
               raise ErrorException(exception = Exception(error_message), code = ErrorException.GENERIC_ERROR, info = error_message)
       except Exception as err:
           raise err

   def checkValuesInList(self, value, field_name, valueList, required = False):
       try:
           if (required):
               self.checkRequired(value, field_name)

           if value is None :
               return None
           elif value in valueList:
               return value
           else:
               error_message = "Field {} can only contain a value between {}".format(field_name, valueList)
               raise ErrorException(exception = Exception(error_message), code = ErrorException.GENERIC_ERROR, info = error_message)
       except Exception as err:
           raise err


   '''
   def checkValuesInList(self, value, field_name, valueList, required = False):
       try:
           if (required and value is None):
               self.checkRequired(value, field_name)
           elif (required and value is not None):
               if (value in valueList):
                   return value
               else:
                   error_message = "Field {} can only contain a value between [{}]".format(field_name, valueList)
                   raise ErrorException(exception=None, code=ErrorException.GENERIC_ERROR, info=error_message)
           elif (value is None):
               return None
           elif (value in valueList):
               return value
           else:
               error_message = "Field {} can only contain a value between [{}]".format(field_name, valueList)
               raise ErrorException(exception = None, code = ErrorException.GENERIC_ERROR, info = error_message)
       except Exception as err:
           raise err

   def checkString(self, value, field_name, required = False):
       try:
           if (required and value is None):
               self.checkRequired(value, field_name)
           elif (required and value is not None):
               if isinstance(value, str):
                   return value
               else:
                   error_message = "Required type for field {} is String".format(field_name)
                   raise ErrorException(exception=None, code=ErrorException.GENERIC_ERROR, info=error_message)
           elif (value is None):
               return None
           if isinstance(value, str):
               return value
           else:
               error_message = "Required type for field {} is String".format(field_name)
               raise ErrorException(exception = None, code = ErrorException.GENERIC_ERROR, info = error_message)
       except Exception as err:
           raise err

   def checkNumber(self, value, field_name, required = False):
       try:
           if (required and value is None):
               self.checkRequired(value, field_name)
           elif(required and value is not None):
               if (isinstance(value, float) or isinstance(value, int) or isinstance(value, complex)):
                   return value
               else:
                   error_message = "Required type for field {} is a number of any type".format(field_name)
                   raise ErrorException(exception=None, code=ErrorException.GENERIC_ERROR, info=error_message)
           elif (value is None):
               return None
           elif (isinstance(value, float) or isinstance(value, int) or isinstance(value, complex)):
               return value
           else:
               error_message = "Required type for field {} is a number of any type".format(field_name)
               raise ErrorException(exception = None, code = ErrorException.GENERIC_ERROR, info = error_message)
       except Exception as err:
           raise err
   '''


##############################################################################################
##############################################################################################
from pyspark.sql.utils import AnalysisException, IllegalArgumentException


class IngestionException(Exception):
    def __init__(self, exception, context:str=None, method=None, code:str=None, extra=None):
        self.exception = exception  # eccezione generata
        self.name = type(exception).__name__  # nome eccezione generata
        self.context = context  # contesto in cui si è verificata l'eccezione (hive, database, applicazione)
        self.method = method  # metodo che ha generato l'eccezione
        self.code = code  # codice associato al tipo di eccezione
        self.extra = extra  # informazioni extra

    def __str__(self):
        return "IngestionException(exception={}, name={}, context={}, method={}, code={}, extra={})"\
            .format(self.exception, self.name, self.context, self.method, self.code, self.extra)

uncaught_exceptions=[]
class ExceptionManager:
    CONTEXT_DATABASE = "DATABASE"
    CONTEXT_HIVE = "HIVE"
    CONTEXT_HDFS = "HDFS"
    CONTEXT_APPLICATION = "APPLICATION"
    CONTEXT_GENERIC = "GENERIC"
    #CONTEXT_CONFIGURATION = "CONFIGURATION"

    """HdfsErrorCode=Enum(names="HDFS_GENERIC_ERROR " 
                       "HDFS_CONFIGURATION_ERROR "
                       "HDFS_CANNOT_CONNECT "
                       "HDFS_CANNOT_WRITE "
                       "HDFS_CANNOT_READ "
                       "HDFS_FILE_NOT_FOUND "
                       ,start=10)
    """

    class HdfsErrorCode(str,Enum):
        HDFS_GENERIC_ERROR="10"
        HDFS_CONFIGURATION_ERROR="11"
        HDFS_CANNOT_CONNECT="12"
        HDFS_CANNOT_WRITE="13"
        HDFS_CANNOT_READ="14"
        HDFS_FILE_NOT_FOUND="15"

    class HiveErrorCode(str, Enum):
        HIVE_GENERIC_ERROR = "20"
        HIVE_CONFIGURATION_ERROR = "21"
        HIVE_TABLE_NOT_FOUND = "22"
        HIVE_CONNECTION_ERROR = "23"
        HIVE_CANNOT_INSERT = "24"
        HIVE_NO_OUTPUT_DATA = "25"
        HIVE_NO_INPUT_DATA = "26"
        HIVE_CANNOT_READ = "27"
        HIVE_CANNOT_WRITE = "28"

    class DatabaseErrorCode(str, Enum):
        DATABASE_GENERIC_ERROR = "30"
        DATABASE_CONFIGURATION_ERROR = "31"
        DATABASE_CONNECTION_ERROR = "32"
        DATABASE_CANNOT_READ = "33"
        DATABASE_CANNOT_WRITE = "34"

    '''
    class ConfigurationErrorCode(str, Enum):
        CONFIGURATION_GENERIC_ERROR = "40"
        CONFIGURATION_PARAMETER_ERROR = "41"
        CONFIGURATION_INITIALIZE_ERROR = "42"
    '''

    class ApplicationErrorCode(str, Enum):
        APPLICATION_GENERIC_ERROR = "50"
        APPLICATION_CONFIGURATION_ERROR = "51"
        APPLICATION_EXECUTION_ERROR = "52"
        APPLICATION_IMPORT_ERROR = "53"
        APPLICATION_TYPE_ERROR = "54"
        APPLICATION_RUNTIME_ERROR = "55"
        SPARK_GENERIC_ERROR = "57"
        SPARK_EXCEPTION = "58"

    class GenericErrorCode(str, Enum):
        MULTIPLE_ERRORS = "60"

    def __init__(self):
        self.exceptions_occurred = []
        sys.excepthook = self.handle_uncaught_exception

    def getNumberOfExceptions(self) -> int:
        return len(self.exceptions_occurred)

    def isErrorFree(self):
        return len(self.exceptions_occurred) == 0

    def getExceptionCode(self, code_lv1: str = "00", code_lv2:str=None) -> str:
        if self.isErrorFree():
            return "0" # in teoria non dovrebbe entrarci mai
        elif code_lv2 is not None:
            code_lv2 = code_lv2
        elif self.getNumberOfExceptions() > 1:
            code_lv2 = self.GenericErrorCode.MULTIPLE_ERRORS
        else:
            code_lv2 = self.ApplicationErrorCode.APPLICATION_GENERIC_ERROR
            e = self.exceptions_occurred[0]
            if isinstance(e, IngestionException):
                code_lv2 = e.code

        if not isinstance(code_lv1, str) or not isinstance(code_lv2, str):
            raise TypeError("Code type must be 'str'")

        return code_lv1 + code_lv2

    # Metodo che costruisce e restituisce un IngestionException partendo dalla eccezione
    # Se è già IngestionException la ritorna senza eseguire operazioni
    def buildIngestionException(self, e, context: str, method:str=None, extra=None):
        if isinstance(e, IngestionException):
            return e

        exception_type = type(e)
        code = self.ApplicationErrorCode.APPLICATION_GENERIC_ERROR

        # ricavo il codice di errore a seconda dell'eccezione sollevata
        if context == self.CONTEXT_HIVE:
            code = self.HiveErrorCode.HIVE_GENERIC_ERROR
            code = self.HiveErrorCode.HIVE_CONFIGURATION_ERROR if exception_type == AssertionError else code
            code = self.HiveErrorCode.HIVE_CANNOT_WRITE if exception_type == AnalysisException else code
            code = self.HiveErrorCode.HIVE_CANNOT_WRITE if exception_type == IllegalArgumentException else code
        elif context == self.CONTEXT_DATABASE:
            code = self.DatabaseErrorCode.DATABASE_GENERIC_ERROR
            code = self.DatabaseErrorCode.DATABASE_CONFIGURATION_ERROR if exception_type == AssertionError else code
            #code = self.DatabaseErrorCode.DATABASE_CONNECTION_ERROR if isinstance(exception_type, jaydebeapi.DatabaseError) else code
            code = self.DatabaseErrorCode.DATABASE_CONNECTION_ERROR if exception_type == Db.DatabaseError else code
            code = self.DatabaseErrorCode.DATABASE_CANNOT_WRITE if exception_type == IllegalArgumentException else code
        elif context == self.CONTEXT_HDFS:
            code = self.HdfsErrorCode.HDFS_GENERIC_ERROR
            code = self.HdfsErrorCode.HDFS_CONFIGURATION_ERROR if exception_type == AssertionError else code
        elif context == self.CONTEXT_APPLICATION:
            code = self.ApplicationErrorCode.APPLICATION_GENERIC_ERROR
            code = self.ApplicationErrorCode.APPLICATION_CONFIGURATION_ERROR if exception_type == AssertionError else code

        # cancella
        # exceptionList = {
        #     "SQLExceptionPyRaisable": IngestionException.ORACLE_CONNECTION_ERROR,
        #     "SQLRecoverableExceptionPyRaisable": IngestionException.ORACLE_CONNECTION_ERROR,
        #     "RuntimeExceptionPyRaisable": IngestionException.ORACLE_CONNECTION_ERROR,
        #     "BatchUpdateExceptionPyRaisable": IngestionException.ORACLE_GENERIC_ERROR,
        #     "DatabaseError": IngestionException.ORACLE_GENERIC_ERROR,
        #     "FileNotFoundError": IngestionException.HDFS_READ_WRITE_ERROR,
        #     "LookupError": IngestionException.HDFS_READ_ERROR,
        #     "KeyError": IngestionException.HDFS_READ_ERROR,
        #     "JSONDecodeError": IngestionException.APPLICATION_GENERIC_ERROR,
        #     "TypeError": IngestionException.APPLICATION_GENERIC_ERROR,
        #     "AttributeError": IngestionException.APPLICATION_GENERIC_ERROR,
        #     "ValueError": IngestionException.APPLICATION_GENERIC_ERROR,
        #     "OverflowError": IngestionException.APPLICATION_GENERIC_ERROR,
        #     "Py4JJavaError": IngestionException.APPLICATION_GENERIC_ERROR,
        #     "IllegalArgumentException": IngestionException.APPLICATION_GENERIC_ERROR,
        #     "NoSectionError": IngestionException.APPLICATION_GENERIC_ERROR
        # }

        ingestion_exception = IngestionException(e, context=context, method=method, code=code, extra=extra)
        self.exceptions_occurred.append(ingestion_exception)

        return ingestion_exception

    @staticmethod
    def handle_uncaught_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return

        print("ERROR")

        lsb = traceback.extract_tb(exc_traceback)

        print(f"ExceptionManager - handle_uncaught_exception exc_type={exc_type}, \
                                                                              exc_value={exc_value},\
                                                                              exc_traceback={lsb}")
        print("ERROR")
        uncaught_exceptions.append(dict(exc_type=exc_type, exc_value=exc_value, exc_traceback=lsb))

    def __str__(self):
        return json.dumps(dict(exceptions_occurred=[str(exc) for exc in self.exceptions_occurred],
                               uncaught_exceptions=[str(exc) for exc in uncaught_exceptions]), indent=2)



##############################################################################################
############################     GESTIONE ECCEZIONI END      #################################
##############################################################################################

# gestione eccezione non gestita
old_exception = sys.excepthook
def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass( exc_type, KeyboardInterrupt ):
        sys.__excepthook__( exc_type, exc_value, exc_traceback )
        return

    hdfs_date_format = datetime.today().strftime( '%Y%m%d' )

    local_log_filename = "/tmp/handle_exception.log"
    formatter = logging.Formatter( '%(asctime)s\t- %(name)s\t- %(levelname)s\t- (%(threadName)-10s)\t- %(message)s',
                                   "%Y-%m-%d %H:%M:%S" )
    mylogger_handler = logging.FileHandler( local_log_filename )
    mylogger_handler.setLevel( logging.INFO )
    mylogger_handler.setFormatter( formatter )

    mylogger = logging.getLogger( "ERROR" )
    mylogger.addHandler( mylogger_handler )

    ENABLE_PRINT = os.environ.get( 'ENABLE_PRINT' )
    if ENABLE_PRINT is not None:
        print( 'ENABLE_PRINT = ' + ENABLE_PRINT )

    mylogger.error( "Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback) )
    mylogger.removeHandler( mylogger_handler )
    mylogger_handler.flush()
    mylogger_handler.close()
    old_exception(exc_type, exc_value, exc_traceback)

sys.excepthook = handle_exception
