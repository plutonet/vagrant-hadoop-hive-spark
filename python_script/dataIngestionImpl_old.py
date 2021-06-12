import json
import sys
from concurrent.futures import ThreadPoolExecutor
from utility import Common, Hdfs, Spark, DbLogger, Logger, Http, Db, Ftp, SshClient, ExceptionManager, IngestionException, TimingAndProfile
import traceback
from typing import NoReturn
from abc import abstractmethod
from dataIngestionInterface import DataIngestionInterface
from utility import ErrorException, MyTrasp
from pathlib import Path
from datetime import datetime
import importlib
import uuid
from enum import Enum
import inspect
from zeep import Client, Transport
from requests.auth import HTTPBasicAuth  # or HTTPDigestAuth, or OAuth1, etc.
from requests import Session
import operator
import zeep
import zipfile
import io
import paramiko
import ftplib
import urllib.parse as urlparse
from urllib.parse import parse_qs
from confluent_kafka import Consumer
import requests
from requests.auth import HTTPDigestAuth
from pyspark.sql import SQLContext
#from kafka import KafkaConsumer

"""
Versione 1.1 
ultima Modifca : Giuseppe 17/07/2019
aggiunta: timeout ReadData

v 1.2 Antonino
- modifica delle impl - manageException(...., info=reqObj)  reqObj = oggetto request passata alla corrispondente funzione
- modifica readDataImpl - aggiunta resp.info(e.info) nell'except
- modifica readDataResponse - aggiunta campo _info

v 1.3 Antonino
- modifica buildDataFrame 29/10/2019
- gestione app_stop_and_continue_if_except in manageException() e executeMethod() 18/11/2019

v 1.4 Michele 09/01/2020
- nel metodo writeDataToCSVImpl aggiunta rename file csv.

v 1.x.5 Giuseppe
- modifica endOK usando writeDataToHdfsRequestList

v 1.6 Michele 08/03/2021
- modifica Kafka
"""

# ENUM
class RequestType(str, Enum):
    SOAP = "SOAP"  # XML
    JSON = "JSON"  # REST
    ZIP = "ZIP"
    FOLDER = "FOLDER"
    FILE = "FILE"
    SOAP_REQUEST = "SOAP_REQUEST"
    JSON_REQUEST_DIGEST_AUTH = "JSON_REQUEST_DIGEST_AUTH"
    JSON_REQUEST_BASIC_AUTH = "JSON_REQUEST_BASIC_AUTH"
    JSON_REQUEST_BASIC_AUTH_PAGED = "JSON_REQUEST_BASIC_AUTH_PAGED"
    DB_REQUEST="DB_REQUEST"

class ProtocolType(str, Enum):
    HTTP = "HTTP"
    FTP = "FTP"
    SFTP = "SFTP"
    SSH = "SSH"
    KAFKA_SSL = "KAFKA_SSL"

class DbmsType(str, Enum):
    ORACLE = "ORACLE"
    POSTGRES = "POSTGRES"

class HiveFileFormat(str, Enum):
    ORC = "orc"
    TEXT = "textfile"
    PARQUET = "parquet"
    AVRO = "avro"
#

class Reader:
    def __init__(self):
        self.data = []

    def __call__(self, s):
        self.data.append(s)


class InitParameters:
    def localLogFile(self, localLogFile):
        self._localLogFile = localLogFile
        return self

    def hdfsLogPath(self, hdfslogpath):
        self._hdfslogpath = hdfslogpath
        return self

    def configFile(self, configfile, sezioniconfig):
        self._configfile = configfile
        self._sezioniconfig = sezioniconfig
        return self


# Metodo usate per set e get dei parametri
# get : se non sono passati argomenti restituisce il valore di _attr
# set : se viene passato un argomento viene settato ad _attr e viene restituito il eslf della classe 

class Base:  # proposta IMIR
    def __init__(self, values={}):
        d = self.__dict__()
        for k in values:
            if k in d:
                d[k] = values[k]


class BaseRequest:
    _key = None
    _key2 = None

    def __init__(self):
        key = id(self)
        key2 = id(self)

    def key(self, *args):
        return self._return(self, "_key", *args)

    def key2(self, *args):
        return self._return(self, "_key2", *args)

    def _return(self, _class, _attr, *_args):
        if len(_args) == 0:
            return getattr(_class, _attr)
        else:
            setattr(_class, _attr, _args[0])
            return _class


class BaseResponse(BaseRequest):
    _exit_code = 0

    def exit_code(self, *args):
        return self._return(self, "_exit_code", *args)


class ReadDataRequest(BaseRequest):
    _url = None
    _user = None
    _password = None
    _type = None
    _method = None
    _filename = None
    _payload = None
    _timeout = None
    _ca_location = None
    _key_location = None
    _certificate_location = None
    _next_call = None

    def url(self, *args):
        return self._return(self, '_url', *args)

    def user(self, *args):
        return self._return(self, '_user', *args)

    def password(self, *args):
        return self._return(self, '_password', *args)

    def type(self, *args):
        return self._return(self, '_type', *args)

    def method(self, *args):
        return self._return(self, '_method', *args)

    def filename(self, *args):
        return self._return(self, '_filename', *args)

    def payload(self, *args):
        return self._return(self, '_payload', *args)

    def timeout(self, *args):
        return self._return(self, '_timeout', *args)

    def ca_location(self, *args):
        return self._return(self, '_ca_location', *args)

    def key_location(self, *args):
        return self._return(self, '_key_location', *args)

    def certificate_location(self, *args):
        return self._return(self, '_certificate_location', *args)
        
    def next_call(self, *args):
        return self._return(self, '_next_call', *args)

class ReadDataResponse(BaseResponse):
    _data = None
    _obj = None
    _info = None
    _filename = None
    _type = None

    def data(self, *args):
        return self._return(self, '_data', *args)

    def filename(self, *args):
        return self._return(self, '_filename', *args)

    def obj(self, *args):
        return self._return(self, '_obj', *args)

    def info(self, *args):
        return self._return(self, '_info', *args)

    def type(self, *args):
        return self._return(self, '_type', *args)


class BuildDataFrameRequest(BaseRequest):
    _data = None
    _obj = None

    def data(self, *args):
        return self._return(self, '_data', *args)

    def obj(self, *args):
        return self._return(self, '_obj', *args)


class BuildDataFrameResponse(BaseResponse):
    _df_hive = None
    _df_csv = None
    _df_db = None

    def df_hive(self, *args):
        return self._return(self, '_df_hive', *args)

    def df_csv(self, *args):
        return self._return(self, '_df_csv', *args)

    def df_db(self, *args):
        return self._return(self, '_df_db', *args)

    def df(self, df):
        self._df_hive = df
        self._df_csv = df
        self._df_db = df
        return self


class WriteDataToHdfsRequest(BaseRequest):
    _dataList = None
    _filename = None
    _path = None
    _type = None

    def dataList(self, *args):
        return self._return(self, '_dataList', *args)

    def filename(self, *args):
        return self._return(self, '_filename', *args)

    def path(self, *args):
        return self._return(self, '_path', *args)

    def type(self, *args):
        return self._return(self, '_type', *args)


class WriteDataToCSVRequest(BaseRequest):
    _data = None
    _filename = None
    _path = None
    _hasHeader = True
    _separator = ','
    _newFilename = None

    def data(self, *args):
        return self._return(self, '_data', *args)

    def filename(self, *args):
        return self._return(self, '_filename', *args)

    def path(self, *args):
        return self._return(self, '_path', *args)

    def separator(self, *args):
        return self._return(self, '_separator', *args)

    def hasHeader(self, *args):
        return self._return(self, '_hasHeader', *args)

    def newFilename(self, *args):
        return self._return(self, '_newFilename', *args)


class WriteDataToHiveRequest(BaseRequest):
    _data = None
    _mode = None
    _location = None
    _table = None
    _partition_fields = None
    _file_format = None
    _schema = None

    def data(self, *args):
        return self._return(self, '_data', *args)

    def mode(self, *args):
        return self._return(self, '_mode', *args)

    def location(self, *args):
        return self._return(self, '_location', *args)

    def table(self, *args):
        return self._return(self, '_table', *args)

    def partition_fields(self, *args):
        return self._return(self, '_partition_fields', *args)

    def file_format(self, *args):
        return self._return(self, '_file_format', *args)

    def schema(self,*args):
        return self._return( self, '_schema', *args )


class WriteDataToDbRequest(BaseRequest):
    _data = None
    _table = None

    def data(self, *args):
        return self._return(self, '_data', *args)

    def table(self, *args):
        return self._return(self, '_table', *args)


class WriteDataToDbSparkRequest(WriteDataToDbRequest):
    _url = None
    _properties = None
    _mode = None

    def url(self, *args):
        return self._return(self, '_url', *args)

    def properties(self, *args):
        return self._return(self, '_properties', *args)

    def mode(self, *args):
        return self._return(self, '_mode', *args)


'''
class WriteDataToOracleCxRequest(WriteDataToOracleRequest):
    _query=None
    _fields=None

    def query(self,*args):
        return self._return(self,'_query',*args)
    def fields(self,*args):
        return self._return(self,'_fields',*args)
'''


class CreateHiveTableRequest(BaseRequest):
    _db = None
    _schema = None
    _table = None
    _mode = None
    _location = None
    _partition_fields = None
    _file_format = None

    def db(self, *args):
        return self._return(self, '_db', *args)

    def schema(self, *args):
        return self._return(self, '_schema', *args)

    def table(self, *args):
        return self._return(self, '_table', *args)

    def mode(self, *args):
        return self._return(self, '_mode', *args)

    def location(self, *args):
        return self._return(self, '_location', *args)

    def partition_fields(self, *args):
        return self._return(self, '_partition_fields', *args)

    def file_format(self, *args):
        return self._return(self, '_file_format', *args)

class ReadDataResponse(BaseResponse):
    _data = None
    _obj = None
    _info = None
    _filename = None
    _type = None

    def data(self, *args):
        return self._return(self, '_data', *args)

    def filename(self, *args):
        return self._return(self, '_filename', *args)

    def obj(self, *args):
        return self._return(self, '_obj', *args)

    def info(self, *args):
        return self._return(self, '_info', *args)

    def type(self, *args):
        return self._return(self, '_type', *args)


# MAIN CLASS
class DataIngestionImpl(DataIngestionInterface):
    @TimingAndProfile()
    def initVar(self):
        # li setto ora a false, nel caso in cui vada in errore la lettura del config, per gestire l'eccezione
        self.app_stop_and_continue_if_except = False
        self.app_continue_if_except = False

        CONFIG_COMMON_FILE = "common.json"
        CONFIG_CUSTOM_FILE = "conf.json"
        try:
            # leggo configurazioni comuni
            with open(CONFIG_COMMON_FILE, 'r') as c_json:
                common_cfg = json.loads(c_json.read())

            # leggo configurazioni del sigolo scripts
            with open(CONFIG_CUSTOM_FILE, 'r') as c_json:
                custom_cfg = json.loads(c_json.read())
        except Exception as e:
            print("initVar - ERROR") # qua il logger non è ancora definito
            raise self.manageException(e, info="ERROR JSON decode: unable to decode json file... check {} or {}".format(CONFIG_COMMON_FILE, CONFIG_CUSTOM_FILE))

        #cfg = {**common_cfg, **custom_cfg} sovrascrive l'intero dict, non il singolo valore
        cfg = self.common.merge_dicts(common_cfg, custom_cfg)
        self.cfg = cfg

        # DEBUG PARAMS
        # se false, tutte le operazioni di scrittura, vengono disattivate (per debug)
        self.enableOperations = self.common.getJsonValue(cfg, "debug", "write_operations", defval=True)
        self.debug_print = self.common.getJsonValue(cfg, "debug", "print", defval=False)
        self.debug_timing_and_profile = self.common.getJsonValue(cfg, "debug", "timing_and_profile", defval=False)

        # LOG PARAMS
        self.log_filename = self.common.getStringTodayFormat(self.common.getJsonValue(cfg, "log", "filename"))
        self.log_local_folder = self.common.getStringTodayFormat(self.common.getJsonValue(cfg, "log", "local_folder"))
        self.log_remote_folder = self.common.getStringTodayFormat(self.common.getJsonValue(cfg, "log", "remote_folder"))

        # APP PARAMS
        self.app_name = self.common.getJsonValue(cfg, "app", "name", defval="app_name")
        self.app_code = str(self.common.getJsonValue(cfg, "app", "code", defval=-1))
        self.exit_code = self.common.getJsonValue(cfg, "app", "exit_code", defval=0)
        self.app_validity_minutes = self.common.getJsonValue(cfg, "app", "validity_minutes", defval=-1)
        self.app_description = self.common.getJsonValue(cfg, "app", "description", defval="")
        self.app_stop_and_continue_if_except = self.common.getJsonValue(cfg, "app", "stop_and_continue_if_except",
                                                                        defval=False)
        self.app_continue_if_except = self.common.getJsonValue(cfg, "app", "continue_if_except", defval=False)
        self.app_init_spark = self.common.getJsonValue(cfg, "app", "init_spark", defval=True)
        self.app_db_logger = self.common.getJsonValue(cfg, "app", "db_logger", defval=True)
        self.app_max_pool_thread = self.common.getJsonValue(cfg, "app", "max_pool_thread", defval=3)
        self.app_readdata_threaded = self.common.getJsonValue(cfg, "app", "readdata_threaded", defval=False)
        self.app_builddataframe_threaded = self.common.getJsonValue(cfg, "app", "builddataframe_threaded", defval=False)
        self.app_builddataframe_union = self.common.getJsonValue(cfg, "app", "builddataframe_union", defval=False)
        # value = -1 -> nessun limite
        self.app_builddataframe_union_limit_df_size = self.common.getJsonValue(cfg, "app",
                                                                               "builddataframe_union_limit_df_size",
                                                                               defval=-1)
        self.app_writedatatohdfs_threaded = self.common.getJsonValue(cfg, "app", "writedatatohdfs_threaded",
                                                                     defval=False)
        self.app_writedatatocsv_threaded = self.common.getJsonValue(cfg, "app", "writedatatocsv_threaded", defval=False)
        self.app_writedatatohive_threaded = self.common.getJsonValue(cfg, "app", "writedatatohive_threaded",
                                                                     defval=False)
        self.app_writedatatodb_threaded = self.common.getJsonValue(cfg, "app", "writedatatodb_threaded", defval=False)
        self.app_createhivetable = self.common.getJsonValue(cfg, "app", "createhivetable", defval=False)

        # SPARK PARAMS
        self.spark_conf = {
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "hive.exec.dynamic.partition": "true",
            "hive.exec.dynamic.partition.mode": "nonstrict",
            "hive.exec.max.dynamic.partitions": "1000",
            "hive.exec.max.dynamic.partition.pernode": "100"
        }

        # HDFS PARAMS
        self.hdfs_url = self.common.getJsonValue(cfg, "hdfs", "url")
        self.hdfs_username = self.common.getJsonValue(cfg, "hdfs", "username")

        # DB PARAMS
        self.db_log_table = self.common.getJsonValue(cfg, "db", "log_table", defval="LOG")
        self.db_log_table_hdfs = self.common.getJsonValue(cfg, "db", "log_table_hdfs", defval="LOG")

        self.db_type = self.common.getJsonValue(cfg, "db", "type")
        self.db_host = self.common.getJsonValue(cfg, "db", "host")
        self.db_port = self.common.getJsonValue(cfg, "db", "port")
        self.db_service = self.common.getJsonValue(cfg, "db", "service")
        self.db_database = self.common.getJsonValue(cfg, "db", "database")
        self.db_schema = self.common.getJsonValue(cfg, "db", "schema")
        #self.db_driver = self.common.getJsonValue(cfg,"db","driver")
        self.db_user = self.common.getJsonValue(cfg, "db", "user")
        self.db_password = self.common.getJsonValue(cfg, "db", "password")
        self.db_jar_filepath = self.common.getJsonValue(cfg, "db", "jar_filepath")

        self.db_driver, self.db_url, self.db_properties = self.getDbParam( \
            type=self.db_type, \
            host=self.db_host, \
            port=self.db_port, \
            service=self.db_service, \
            database=self.db_database, \
            schema=self.db_schema, \
            user=self.db_user, \
            password=self.db_password \
            )
        print("CARICAMENTO JSON OK")
        
    def getDbParam(self, type=None, host=None, port=None, service=None, user=None, password=None, database=None, schema=None):
        driver = ""
        url = ""
        properties = {}

        if type.upper() == DbmsType.ORACLE:
            driver = "oracle.jdbc.OracleDriver"
            url = "jdbc:oracle:thin:@{}:{}/{}".format(host, port, service)
            properties = { \
                'host': host, \
                'port': port, \
                'user': user, \
                'password': password, \
                'service': service, \
                'driver': driver \
                }
        elif type.upper() == DbmsType.POSTGRES:
            driver = "org.postgresql.Driver"
            url = "jdbc:postgresql://{}:{}/{}".format(host, port, database)
            properties = { \
                'host': host, \
                'port': port, \
                'user': user, \
                'password': password, \
                'database': database, \
                'schema': schema, \
                'driver': driver \
                }

        return driver, url, properties

    ## da usare negli script, invece di self.ftp, usa self.getFtpClient()
    def getFtpClient(self, host=None, username=None, password=None):
        if not hasattr(self, "ftpClient") or self.ftpClient is None:
            self.ftpClient = None
            try:
                assert host is not None, "FTP client params - 'host' not set in configuration file"
                assert username is not None, "FTP client params - 'username' not set in configuration file"
                assert password is not None, "FTP client params - 'password' not set in configuration file"
                self.ftpClient = Ftp(host, username, password)
            except Exception as e:
                self.logger.error("getFtpClient - ERROR")
                self.manageException(e, context=ExceptionManager.CONTEXT_APPLICATION)
        return self.ftpClient

    def getSshClient(self, host=None, username=None, password=None, port=None):
        if not hasattr(self, "sshClient") or self.sshClient is None:
            self.sshClient = None
            try:
                assert host is not None, "SSH client params - 'host' not set in configuration file"
                assert username is not None, "SSH client params - 'username' not set in configuration file"
                assert password is not None, "SSH client params - 'password' not set in configuration file"
                assert port is not None, "SSH client params - 'port' not set in configuration file"
                self.sshClient = SshClient(host, username, password, port)
            except Exception as e:
                self.logger.error("getSshClient - ERROR")
                self.manageException(e, context=ExceptionManager.CONTEXT_APPLICATION)
        return self.sshClient

    def getHdfsClient(self, host=None, username=None):
        if not hasattr(self, "hdfsClient") or self.hdfsClient is None:
            self.hdfsClient = None
            try:
                host = host if host else self.hdfs_url
                username = username if username else self.hdfs_username        
                assert host is not None, "HDFS client params - 'url' not set in configuration file"
                assert username is not None, "HDFS client params - 'username' not set in configuration file"
                
                self.hdfsClient = Hdfs(host, username)
            except Exception as e:
                self.logger.error("getHdfsClient - ERROR")
                self.manageException(e, context=ExceptionManager.CONTEXT_HDFS)
        return self.hdfsClient

    def getHttpClient(self):
        if not hasattr(self, "httpClient") or self.httpClient is None:
            self.httpClient = Http()
        return self.httpClient

    def getDbClient(self, driver=None, url=None, user=None, password=None, jar_filepath=None, schema=None):
        if not hasattr(self, "dbClient") or self.dbClient is None:
            self.dbClient = None
            try:
                driver = driver if driver else self.db_driver
                url = url if url else self.db_url
                user = user if user else self.db_user
                password = password if password else self.db_password
                jar_filepath = jar_filepath if jar_filepath else self.db_jar_filepath
                schema = schema if schema else self.db_schema
                assert driver is not None, "DB client params - 'driver' not set in configuration file"
                assert url is not None, "DB client params - 'url' not set in configuration file"
                assert user is not None, "DB client params - 'user' not set in configuration file"
                assert password is not None, "DB client params - 'password' not set in configuration file"
                assert jar_filepath is not None, "DB client params - 'jar_filepath' not set in configuration file"
                assert schema is not None, "DB client params - 'schema' not set in configuration file"
                
                self.dbClient = Db(driver, url, user, password, jar_filepath, schema)
            except Exception as e:
                self.logger.error("getDbClient - ERROR")
                raise self.manageException(e, context=ExceptionManager.CONTEXT_DATABASE)

        return self.dbClient

    def getSparkClient(self):
        if self.app_init_spark and hasattr(self, "spark") and self.spark is not None:
            return self.spark
    ##

    def __init__(self):
        self.script_start_time = datetime.now()
        self.exception_manager = ExceptionManager()
        self.common = Common()

        try:
            '''
            #Lettura parametri nel common.ini comune a tutti
            config = configparser.ConfigParser()
            config.read("common.ini")
            self.common.initializeVar(self, config)
            # Lettura parametri nel config.ini

            file_ini = "config.ini"
            file_py = "parameter.py"
            if hasattr( initParam, "_configfile" ) :
                est=initParam._configfile.split( "." )[1]
                if  est == "py":
                    file_py = initParam._configfile
                elif est == "ini":
                    file_ini = initParam._configfile


            if Path(file_ini).exists() :
                print("CONFIG.ini" )
                config.read(file_ini)
                self.common.initializeVar(self, config, initParam._sezioniconfig)
            elif Path(file_py).exists():
                print("PARAMETER.PY")
                parameter=importlib.import_module(file_py.split(".")[0])
                para=parameter.Parameter()
                #print([v for k,v in para.__dict__.items() if not k.startswith("__")])
                for var in dir( para ):
                    if not var.startswith( "__" ):
                       setattr( self, var, getattr( para, var ) )
            '''
            # inizializzo tutte le variabili dai file json di configurazione
            self.initVar()

            # init logger
            local_log = self.log_local_folder + "/" + self.log_filename
            remote_log = self.log_remote_folder + "/" + self.log_filename
            self.logger = Logger(class_name='', app_name=self.app_name, local_log_file=local_log, time_precision='second', enable_debug=self.debug_print) if local_log else None
            self.logger.info("init - local_log: {}".format(local_log))
            self.logger.info("init - remote_log: {}".format(remote_log))

            # init hdfs client
            self.getHdfsClient(host=self.hdfs_url, username=self.hdfs_username)

            # init db e dbLogger
            self.initDatabase(remote_log=remote_log)
            print("CARICAMENTO PRE SPARK")
            # init spark
            if self.app_init_spark:
                self.initSpark()

        except Exception as e:
            self.logger.error("DataIngestionImpl.__init__ - ERROR") if hasattr(self, "logger") else print("DataIngestionImpl.__init__ - ERROR")
            # da cancellare.. se gestite bene, a questo livello dovrebbero essere tutte IngestionException
            if not isinstance(e, IngestionException):
                traceback.print_exc()  # dovresti fare la raise cosi va nell'handle_uncaught_exception()
                raise e
            self.app_code = self.app_code if hasattr(self, "app_code") else "00" # potrebbe essere andato in errore prima di leggere l'app code
            self.exit_code = self.exception_manager.getExceptionCode(code_lv1=self.app_code)
            self.endKO(e, self.exit_code)


    def initDatabase(self, remote_log=None):
        try:
            self.logger.debug("driver: {}".format(self.db_driver))
            self.logger.debug("url: {}".format(self.db_url))
            self.logger.debug("user: {}".format(self.db_user))
            self.logger.debug("password: {}".format(self.db_password))
            self.logger.debug("jar_filepath: {}".format(self.db_jar_filepath))

            self.getDbClient(self.db_driver, self.db_url, self.db_user, self.db_password, self.db_jar_filepath,
                         schema=self.db_schema)

            if self.app_db_logger:
                self.run_id = self.common.generateId()

                self.logger.debug("db: {}".format(self.dbClient))
                self.logger.debug("db_log_table: {}".format(self.db_log_table))
                self.logger.debug("run_id: {}".format(self.run_id))
                self.logger.debug("app_name: {}".format(self.app_name))
                self.logger.debug("app_description: {}".format(self.app_description))
                self.logger.debug("remote_log: {}".format(remote_log))

                self.dbLogger = DbLogger(self.dbClient, self.db_log_table, self.run_id,
                                         self.app_name, self.app_description,
                                         remote_log,log_table_hdfs=self.db_log_table_hdfs) if remote_log and self.enableOperations else None

                self.dbLogger.logStartExecution(int(self.app_validity_minutes)) if self.enableOperations else None
        except Exception as e:
            self.logger.error("initDatabase - ERROR")
            raise self.manageException(e)

    @TimingAndProfile()
    def initSpark(self):
        try:
            print("initSpark - INIT")
            self.logger.info("initSpark - INIT")
            self.spark = Spark(self.app_name, self.spark_conf)
            print( "Caricamento SPARK OK" )
            self.spark.getSparkContext().setLogLevel("ERROR")
            self.dbLogger.setAppId(self.spark.getApplicationID()) if (self.enableOperations and self.app_db_logger) else None
            self.logger.info("initSpark - END")
            print( "initSpark - END" )
        except Exception as e:
            self.logger.error("initSpark - ERROR")
            raise self.manageException(e)

    # restituisce val2 se : val1 is None , val2 esiste e not is None
    '''
    def getparamold(self, val1, val2):
        return getattr(self, val2) \
            if (val1 is None and hasattr(self, val2) and getattr(self, val2) is not None) \
            else val1
    '''

    # restituisce val2 se : val1 is None , val2 esiste e not is None
    def getParam(self, *args):
        return self.common.getJsonValue(self.cfg, *args)

    def getPayloadFromRequest(self, requestObj):
        request_key = requestObj.key()
        for r in self.readDataRequestList:
            if r.key() == request_key:
                return r.payload()
        return None

    def getHiveSchemaFromRequest(self, requestObj):
        request_key1 = requestObj.key()
        request_key2 = requestObj.key2()
        for req in self.writeDataToHiveRequestList:
            if req.key() == request_key1 and req.key() == request_key2:
                return req.schema()
        return None

    def clearRequests(self, succesful_key_list):
        self.readDataRequestList = [req for req in self.readDataRequestList if req.key() in succesful_key_list]
        self.writeDataToHdfsRequestList = [req for req in self.writeDataToHdfsRequestList if
                                           req.key() in succesful_key_list]
        self.createHiveTableRequestList = [req for req in self.createHiveTableRequestList if
                                           req.key() in succesful_key_list]
        self.writeDataToCSVRequestList = [req for req in self.writeDataToCSVRequestList if
                                          req.key() in succesful_key_list]
        self.writeDataToHiveRequestList = [req for req in self.writeDataToHiveRequestList if
                                           req.key() in succesful_key_list]
        self.writeDataToDbRequestList = [req for req in self.writeDataToDbRequestList if
                                         req.key() in succesful_key_list]

    def manageException(self, exc, context=None, method=None, info=None):
        print("DEBUG - manageException - INIT") if self.debug_print else None

        # ricavo il nome del metodo che ha generato l'eccezione (considero il caso in cui venga usato un decorator)
        if method is None:
            caller = inspect.stack()[1][3]
            method = inspect.stack()[2][3] if caller == "wrapper" else caller

        # ricavo il contesto dal nome del metodo
        if context is None:
            methods = {
                "initVar": ExceptionManager.CONTEXT_APPLICATION,
                "initDatabase": ExceptionManager.CONTEXT_DATABASE,
                "initRequests": ExceptionManager.CONTEXT_APPLICATION,
                "initSpark": ExceptionManager.CONTEXT_APPLICATION,
                "readDataImpl": ExceptionManager.CONTEXT_APPLICATION,
                "writeDataToHdfsImpl": ExceptionManager.CONTEXT_HDFS,
                "createHiveTableImpl": ExceptionManager.CONTEXT_HIVE,
                "writeDataToHiveImpl": ExceptionManager.CONTEXT_HIVE,
                "writeDataToDbImpl": ExceptionManager.CONTEXT_DATABASE,
                "buildDataFrame": ExceptionManager.CONTEXT_APPLICATION,
                "default": ExceptionManager.CONTEXT_GENERIC,
            }
            context = methods[method] if method in methods else methods["default"]

        # creo una IngestionException
        ingestionExc = self.exception_manager.buildIngestionException(exc, context=context, method=method, extra=info)
        print("DEBUG - manageException - {}".format(ingestionExc)) if self.debug_print else None

        # print traceback exc
        traceback.print_exc() if self.debug_print else None

        print("DEBUG - manageException - END") if self.debug_print else None

        if self.app_stop_and_continue_if_except:
            return 1 # brutto sto return 1
        elif not self.app_continue_if_except:
            raise ingestionExc

        return ingestionExc

    # cancella
    '''
    def manageException_old(self, e, key, response=None, info=None):
        self.logger.info("manageException - INIT")
        self.logger.error(e)
        exc = self.common.getException(e, key)
        self.exit_code = self.common.getExceptionCode(exc, int(self.app_code))

        if response is not None:
            response.exit_code(self.exit_code)

        exc.info = info if info is not None else None

        self.logger.info("manageException - exit_code = {}".format(self.exit_code))

        if self.app_stop_and_continue_if_except:
            return 1
        elif not self.app_continue_if_except:
            raise exc
    '''

    def executeMethod(self, method, requestList, threaded, returned):
        try:
            responseList = []
            requestList = self.common.toList(requestList)
            succesful_key_list = []

            if threaded:
                with ThreadPoolExecutor(max_workers=int(self.app_max_pool_thread)) as executor:
                    if returned:
                        responseList = executor.map(method, requestList)
                    else:
                        executor.map(method, requestList)
            else:
                for requestObj in requestList:
                    if returned:
                        resp = method(requestObj)
                        # gestione app_stop_and_continue_if_except
                        # se il flag è attivo, in caso di errore interrompo il flusso "verticale" e continuo con gli altri flussi
                        if resp == 1:
                            self.clearRequests(succesful_key_list)
                            break
                        else:
                            if resp is not None :
                                self.logger.info("isinstance(resp, list): {}".format(isinstance(resp, list)))
                                self.logger.info("resp: {}".format(resp))
                                if (isinstance(resp, list) and len(resp) == 0):
                                    continue
                                responseList.append(resp)
                                succesful_key_list.append(resp.key() if not isinstance(resp, list) else resp[0].key())
                    else:
                        method(requestObj)

            return list(filter(None, responseList))
        except Exception as e:
            #self.logger.error("executeMethod - KO")
            self.logger.error(f"executeMethod -KO method: {method.__name__}")                                                                 
            raise e

    #def menageReadDataException(self, e, readDataRequestObj, response=None):
    def menageReadDataException(self, e):
        return self.manageException(e)

    @TimingAndProfile()
    def readData(self, readDataRequestList):
        threaded = self.app_readdata_threaded
        self.logger.info("readData - threaded = {}".format(threaded))
        return self.executeMethod(method=self.readDataImpl, requestList=readDataRequestList, threaded=threaded, returned=True)

    def readDataImpl(self, readDataRequestObj):
        try:
            # response
            return_response = ReadDataResponse()

            # request params
            request_key = readDataRequestObj.key()
            request_type = readDataRequestObj.type()
            request_method = readDataRequestObj.method()
            request_filename = readDataRequestObj.filename()
            request_payload = readDataRequestObj.payload()
            request_url = readDataRequestObj.url()
            request_username = readDataRequestObj.user()
            request_password = readDataRequestObj.password()
            request_timeout = readDataRequestObj.timeout()
            request_ca_location = readDataRequestObj.ca_location()
            request_key_location = readDataRequestObj.key_location()
            request_certificate_location = readDataRequestObj.certificate_location()
            request_next_call = readDataRequestObj.next_call()

            self.logger.info("readDataImpl [{}] - INIT".format(request_key))  # if request_key is not None else None
            self.logger.info("readDataImpl [{}] - type = {}".format(request_key, request_type))
            self.logger.info("readDataImpl [{}] - url = {}".format(request_key, request_url))
            self.logger.info("readDataImpl [{}] - method = {}".format(request_key, request_method))
            self.logger.info("readDataImpl [{}] - payload = {}".format(request_key, request_payload))
            self.logger.info("readDataImpl [{}] - user = {}".format(request_key, request_username))
            self.logger.info("readDataImpl [{}] - password = {}".format(request_key, request_password))
            self.logger.info("readDataImpl [{}] - filename = {}".format(request_key, request_filename))
            self.logger.info("readDataImpl [{}] - timeout = {}".format(request_key, request_timeout))
            self.logger.info("readDataImpl [{}] - ca_location = {}".format(request_key, request_ca_location))
            self.logger.info("readDataImpl [{}] - key_location = {}".format(request_key, request_key_location))
            self.logger.info("readDataImpl [{}] - certificate_location = {}".format(request_key, request_certificate_location))
            self.logger.info("readDataImpl [{}] - next_call = {}".format(request_key, request_next_call))

            data_ = None
            filename_ = None
            obj_ = None
            key_2 = request_key

            if request_type.upper() == RequestType.SOAP_REQUEST:
                headers={'content-type': 'text/xml'}
                response = requests.post(request_url,data=request_payload,headers=headers,auth=HTTPBasicAuth(request_username, request_password)) 
                data_=response.content
            
            elif request_type.upper() == RequestType.JSON_REQUEST_DIGEST_AUTH:
                response = requests.get(request_url,auth=HTTPDigestAuth(request_username, request_password))
                data_=response.content
            
            elif request_type.upper() == RequestType.JSON_REQUEST_BASIC_AUTH:
                if request_payload is not None:
                    headers={'Content-Type':'application/json'}
                    response = requests.get(request_url,headers=headers,data=json.dumps(request_payload),auth=HTTPBasicAuth(request_username, request_password))
                    data_=response.content
                else:
                    response = requests.get(request_url,auth=HTTPBasicAuth(request_username, request_password))
                    data_=response.content
            
            elif request_type.upper() == RequestType.DB_REQUEST:
                data_="DB_REQUEST"
            
            elif request_type.upper() == RequestType.JSON_REQUEST_BASIC_AUTH_PAGED:
                #self.logger.info("paged: {}".format(request_url))
                temp_request_url = request_url
                next_request = None
                #self.logger.info("next_request: {}".format(next_request))
                while True:
                    #self.logger.info("true:ciclo")
                    response = requests.get(temp_request_url,auth=HTTPBasicAuth(request_username, request_password))
                    temp_data = response.json()
                    if request_next_call and isinstance(request_next_call, list):
                        if not data_:
                            data_ = []
                        data_.append(temp_data)
                        next_request = temp_data
                        for e in request_next_call:
                            next_request = next_request[e]
                            self.logger.debug("next_request: {}".format(next_request))
                        temp_request_url = next_request    
                    else:
                        data_ = temp_data
                    if (not next_request):
                        break
                self.logger.info("data_: {}".format(data_))
            
            elif request_type.upper() == RequestType.SOAP:
                session = Session()
                session.auth = HTTPBasicAuth(request_username, request_password)
                my_transp = MyTrasp(session=session)
                wsdl = request_url
                client = Client(wsdl=wsdl, transport=my_transp)
                method = getattr(client.service, request_method)

                if request_payload is None:
                    obj_ = method()
                else:
                    obj_ = method(*request_payload)
                data_ = my_transp.res_xml.content
            else:
                protocol = request_url.split("://")[0]
                if protocol.upper() == ProtocolType.SSH:
                    path = request_url[len(protocol) + len("://"):]
                    host_and_port = path.split("/")[0]
                    host = host_and_port.split(":")[0]
                    port = host_and_port.split(":")[1]
                    file_remote = path[len(host_and_port):]
                    user = request_username
                    password = request_password
                    self.logger.info("readDataImpl ssh - host = {}".format(host))
                    self.logger.info("readDataImpl ssh - port = {}".format(port))
                    self.logger.info("readDataImpl ssh - user = {}".format(user))
                    self.logger.info("readDataImpl ssh - password = {}".format(password))
                    self.logger.info("readDataImpl ssh - file_remote = {}".format(file_remote))

                    # host = '10.64.20.19'
                    # port = 22
                    # file_remote = '/home/sdeuser/rome_static_gtfs.zip'
                    # user = 'sdeuser'
                    # password = 'sdeuser.1'

                    '''
                    ssh = paramiko.SSHClient()
                    ssh.set_missing_host_key_policy(
                        paramiko.AutoAddPolicy())  # Set policy to use when connecting to servers without a known host key
                    ssh.connect(hostname=host, username=user, password=password, port=port)

                    with ssh.open_sftp() as sftp:
                        # file_local = 'test1.zip'
                        # sftp.get(file_remote, file_local)
                        # sftp.put(localpath, remotepath)

                        if request_type.upper() == RequestType.FOLDER:
                            self.logger.info("readDataImpl ssh - FOLDER")
                            # TODO
                            #
                            #
                            #
                        elif request_type.upper() == RequestType.FILE:
                            self.logger.info("readDataImpl ssh - FILE")
                            with sftp.file(file_remote, mode='rb') as file:
                                data_ = file.read()
                                filename_ = file_remote
                    ssh.close()
                    '''

                    if request_type.upper() == RequestType.FOLDER:
                        self.logger.info("readDataImpl ssh - FOLDER")
                        # TODO
                        #
                        #
                    # elif request_type.upper() == RequestType.FILE:
                    elif request_type.upper() in (RequestType.FILE, RequestType.ZIP):
                        data_ = self.getSshClient(host,user,password,port).getFileContentSFTP(file_remote)
                        filename_ = file_remote
                elif protocol.upper() == ProtocolType.KAFKA_SSL:
                    
                    parsed = urlparse.urlparse(request_url)
                    topic=parse_qs(parsed.query)["topic"]
                    topic = ''.join(topic)
                    group_id=parse_qs(parsed.query)["group_id"]
                    group_id = ''.join(group_id)
                    path=parsed.path
                    broker= path[len(protocol) + len("://"):]

                    self.logger.info("readDataImpl kafka - broker = {}".format(broker))
                    self.logger.info("readDataImpl kafka - topic = {}".format(topic))
                    self.logger.info("readDataImpl kafka - group_id = {}".format(group_id))
                    
                    data_=self.getMessageFromKafka(broker,group_id,request_ca_location,request_certificate_location,request_key_location,topic)
                    
                elif protocol.upper() == ProtocolType.FTP:
                    path = request_url[len(protocol) + len("://"):]
                    host_and_port = path.split("/")[0]
                    host = host_and_port.split(":")[0]
                    port = host_and_port.split(":")[1]
                    file_remote = path[len(host_and_port):]
                    user = request_username
                    password = request_password
                    # host = "ftp.viasatgroup.net"
                    # user = "rsmroma"
                    # password = "Vitol92N"
                    self.logger.info("readDataImpl ftp - host = {}".format(host))
                    self.logger.info("readDataImpl ftp - port = {}".format(port))
                    self.logger.info("readDataImpl ftp - user = {}".format(user))
                    self.logger.info("readDataImpl ftp - password = {}".format(password))
                    self.logger.info("readDataImpl ftp - file_remote = {}".format(file_remote))

                    if request_type.upper() == RequestType.FOLDER:
                        self.logger.info("readDataImpl ftp - FOLDER")
                        self.logger.info("readDataImpl ftp - filename = {}".format(request_filename))
                        name_and_content_file_list = []
                        if request_filename is not None:
                            key_2 = request_filename

                        try:
                            name_and_content_file_list = self.getFtpClient(host,user,password).retriveFilenameAndBinaryFilesFromFolder(
                                filename_filter=request_filename)
                        except Exception as e:
                            self.logger.error("readDataImpl ftp - {}".format(e))
                            pass

                        data_list = [elem['content'] for elem in name_and_content_file_list]
                        filename_list = [elem['filename'] for elem in name_and_content_file_list]
                        self.logger.info("readDataImpl ftp - len(data_list) = {}".format(len(data_list)))
                        self.logger.info("readDataImpl ftp - len(filename_list) = {}".format(len(filename_list)))

                        data_ = data_list
                        filename_ = filename_list
                    elif request_type.upper() == RequestType.FILE:
                        data_ = list(self.getFtpClient(host,user,password).retriveBinaryFile(file_remote))
                        filename_ = file_remote

                elif request_type.upper() == RequestType.JSON:
                    data_ = self.getHttpClient().request(request_url, request_type, request_method, request_payload,
                                              request_timeout)
                
                elif protocol.upper() == ProtocolType.SFTP:
                    if request_type.upper() == RequestType.FILE:
                        self.logger.debug("FILE SFTP")
                        path = request_url.replace("/", "").split(":")
                        self.logger.debug("path: ", path)
                        host, port = path[1], int(path[-1])
                        self.logger.debug("host: {}, port: {}".format(host, port))

                        try:
                            transport = paramiko.Transport((host, port))
                            self.logger.debug("readDataImpl transport - {}".format(str(transport)))
                            transport.connect(username=request_username, password=request_password)
                            self.logger.debug("readDataImpl transport - {}".format(str(transport)))
                            sftp = paramiko.SFTPClient.from_transport(transport)
                            self.logger.debug("readDataImpl sftp - {}".format(str(sftp)))
                            path = request_filename.split("/")
                            if path[-1] in sftp.listdir("/".join(path[:-1])):
                                read_file = sftp.file(request_filename, mode='r')
                                self.logger.debug("readDataImpl sftp - {}".format(str(read_file)))
                                data_ = read_file.read()
                                sftp.remove(request_filename)
                                self.logger.debug("file {} removed".format(request_filename))
                            
                            if sftp: 
                                sftp.close()
                            if transport:
                                transport.close()
                               
                        except Exception as e:    
                            self.logger.error("readDataImpl sftp - {}".format(e))
                            pass
                else:
                    self.logger.warning("NO compatible RequestType")

            return_response.key(request_key) \
                .key2(key_2) \
                .type(request_type) \
                .filename(filename_) \
                .data(data_) \
                .obj(obj_)

            # resp.exit_code(0)
            self.logger.info("readDataImpl [{}] - END".format(request_key))
            return return_response
        except Exception as e:
            self.logger.error("readDataImpl [{}] - KO".format(request_key))
            return self.menageReadDataException(e)

    def getMessageFromKafka(self,broker,group_id,request_ca_location,request_certificate_location,request_key_location,topic) :
        
        #'default.topic.config': {'auto.offset.reset': 'latest'}
        conf_kafka = {'bootstrap.servers': broker,
                      'group.id': group_id,
                      'security.protocol':'SSL',
                      'ssl.ca.location': request_ca_location,
                      'ssl.certificate.location':request_certificate_location,
                      'ssl.key.location':request_key_location,
                      'auto.offset.reset': 'latest',
                      'enable.auto.commit': True                      
                      }
        consumer = Consumer(conf_kafka)
        consumer.subscribe([topic])
        listMsg = consumer.consume(num_messages=5000,timeout=10)
        consumer.close()
        
        listObj=[]
        for msg in listMsg:
            listObj.append(msg.value().decode('utf-8'))
        
        return listObj
    
    
    def readDataSplit(self, readDataResponse):
        if readDataResponse.type() == RequestType.ZIP:
            resp = []

            '''
            if not isinstance(readDataResponse.data() , io.BytesIO) :
                bytes_ = io.BytesIO(readDataResponse.data())
            else :
                bytes_ = readDataResponse.data()
            '''

            bytes_ = io.BytesIO(readDataResponse.data())
            inzipfile = zipfile.ZipFile(bytes_)

            for name in inzipfile.namelist():
                # inzipfile.open(infile,'r').read()
                with inzipfile.open(name) as csv_f:
                    respSingle = ReadDataResponse()
                    csv_f_as_text = io.TextIOWrapper(csv_f)
                    # data(csv_f_as_text.read()).\
                    # data(io.BytesIO(csv_f.read())).\
                    respSingle. \
                        key(readDataResponse.key()). \
                        key2(name). \
                        data(csv_f_as_text.read()). \
                        obj(name)
                    resp.append(respSingle)
            return resp
        else:
            return self.common.toList(readDataResponse)

    def writeDataToCSV(self, writeDataToCSVRequestList):
        threaded = self.app_writedatatocsv_threaded
        return self.executeMethod(self.writeDataToCSVImpl, writeDataToCSVRequestList, threaded, False)

    def writeDataToCSVImpl(self, writeDataToCSVRequestObj):
        try:
            k = writeDataToCSVRequestObj.key()
            self.logger.info("writeDataToCSVImpl [{}] - INIT".format(k))
            self.logger.info("writeDataToCSVImpl [{}] - key: {}".format(k, writeDataToCSVRequestObj.key()))
            self.logger.info("writeDataToCSVImpl [{}] - filename: {}".format(k, writeDataToCSVRequestObj.filename()))
            self.logger.info("writeDataToCSVImpl [{}] - path: {}".format(k, writeDataToCSVRequestObj.path()))
            self.logger.info("writeDataToCSVImpl [{}] - hasHeader: {}".format(k, writeDataToCSVRequestObj.hasHeader()))
            self.logger.info("writeDataToCSVImpl [{}] - separator: {}".format(k, writeDataToCSVRequestObj.separator()))

            if writeDataToCSVRequestObj._data is None:
                self.logger.error("writeDataToCSVImpl [{}] - data is None".format(k))
                return

            df = writeDataToCSVRequestObj.data()
            filename = writeDataToCSVRequestObj.filename()
            path = writeDataToCSVRequestObj.path()
            separator = writeDataToCSVRequestObj.separator()
            hasHeader = writeDataToCSVRequestObj.hasHeader()

            path = self.common.getStringTodayFormat(path)
            filename = self.common.getStringTodayFormat(filename)
            self.logger.info("writeDataToCSVImpl [{}] - filename: {}".format(k, filename))
            self.logger.info("writeDataToCSVImpl [{}] - path: {}".format(k, path))

            # salvataggio file CSV, il nome che attribuisce SPARK e part*
            df.coalesce(1).write \
                .option("sep", separator) \
                .option("header", hasHeader) \
                .format("csv") \
                .save(path)

            # rinomino part* con il filename inserito
            self.getHdfsClient().hdfsMv(path + "part*", path + filename) if self.enableOperations else None
            self.logger.info("writeDataToCSVImpl [{}] - END".format(k))
        except Exception as e:
            self.logger.error("writeDataToCSVImpl [{}] - KO".format(k))
            self.manageException(e, ErrorException.APPLICATION_GENERIC_ERROR, info=writeDataToCSVRequestObj)

    @TimingAndProfile()
    def writeDataToHdfs(self, writeDataToHdfsRequestList):
        threaded = self.app_writedatatohdfs_threaded
        return self.executeMethod(method=self.writeDataToHdfsImpl, requestList=writeDataToHdfsRequestList, threaded=threaded, returned=False)

    def writeDataToHdfsImpl(self, writeDataToHdfsRequestObj):
        try:
            k = writeDataToHdfsRequestObj.key()
            self.logger.info("writeDataToHdfsImpl [{}] - INIT".format(k))
            self.logger.info("writeDataToHdfsImpl [{}] - filename: {}".format(k, writeDataToHdfsRequestObj.filename()))
            self.logger.info("writeDataToHdfsImpl [{}] - path: {}".format(k, writeDataToHdfsRequestObj.path()))
            self.logger.info("writeDataToHdfsImpl [{}] - type: {}".format(k, writeDataToHdfsRequestObj.type()))
        
            if writeDataToHdfsRequestObj._dataList is None:
                self.logger.warning("writeDataToHdfsImpl [{}] - data is None".format(k))
                return
        
            self.logger.info("writeDataToHdfsImpl [{}] - len(writeDataToHdfsRequestObj._dataList): {}".format(k, len(
                writeDataToHdfsRequestObj._dataList)))
            #data_list = self.common.toList(writeDataToHdfsRequestObj._dataList)
            data_list = writeDataToHdfsRequestObj._dataList
            filename_list = self.common.toList(writeDataToHdfsRequestObj._filename)
            append_ = False
            overwrite_ = False
            self.filename_list=[]
            for i in range(len(data_list)):
                # data = writeDataToHdfsRequestObj._dataList
                data = data_list[i]
                #self.logger.info("writeDataToHdfsImpl [{}] - data: {}".format(k, data))
                self.logger.info("writeDataToHdfsImpl [{}] - type data: {}".format(k, type(data)))
                if isinstance(writeDataToHdfsRequestObj._filename, list):
                    file_name = filename_list[i]
                    overwrite_ = True
                else:
                    file_name = writeDataToHdfsRequestObj._filename  # settato nel json
                    # append_ = True
        
                self.logger.info("writeDataToHdfsImpl [{}] - file_name: {}".format(k, file_name))
        
                path = writeDataToHdfsRequestObj._path
                #type = writeDataToHdfsRequestObj._type
        
                file_name = self.common.getStringTodayFormat(file_name)
                path = self.common.getStringTodayFormat(path)
        
                self.logger.info("writeDataToHdfsImpl [{}] - filename_2: {}".format(k, file_name))
                self.logger.info("writeDataToHdfsImpl [{}] - path_2: {}".format(k, path))
        
                self.getHdfsClient().hdfsMkdir(path) if self.enableOperations else None
                # file_origin = self.getHdfsClient().hdfsWriteJson(data, folder_name, file_name)
        
                # data_decode = data.decode(encoding='utf-8', errors="ignore") if isinstance(data, bytes) else data
                # self.getHdfsClient().hdfsWrite(path + file_name, data_decode)
        
                self.getHdfsClient().hdfsWrite(path + file_name, data, append=append_, overwrite=overwrite_) if self.enableOperations else None
                self.filename_list.append(path + file_name)
            self.logger.info("writeDataToHdfsImpl [{}] - END".format(k))
        except Exception as e:
            self.logger.error("writeDataToHdfsImpl [{}] - KO".format(k))
            self.manageException(e)

    @TimingAndProfile()
    def createHiveTable(self, createHiveTableRequestList):
        threaded = self.app_createhivetable
        return self.executeMethod(method=self.createHiveTableImpl, requestList=createHiveTableRequestList, threaded=threaded, returned=False)

    def createHiveTableImpl(self, createHiveTableRequestObj):
        try:
            k = createHiveTableRequestObj.key()
            self.logger.info("createHiveTableImpl [{}] - INIT".format(k))
            self.logger.info("createHiveTableImpl [{}] - db: {}".format(k, createHiveTableRequestObj.db()))
            self.logger.info("createHiveTableImpl [{}] - schema: {}".format(k, createHiveTableRequestObj.schema()))
            self.logger.info("createHiveTableImpl [{}] - table: {}".format(k, createHiveTableRequestObj.table()))
            self.logger.info("createHiveTableImpl [{}] - mode: {}".format(k, createHiveTableRequestObj.mode()))
            self.logger.info("createHiveTableImpl [{}] - location: {}".format(k, createHiveTableRequestObj.location()))
            self.logger.info("createHiveTableImpl [{}] - file_format: {}".format(k, createHiveTableRequestObj.file_format()))
            self.logger.info("createHiveTableImpl [{}] - partition_fields: {}".format(k, createHiveTableRequestObj.partition_fields()))
            self.logger.info("createHiveTableImpl [{}] - partition_fields2: {}".format(k, createHiveTableRequestObj._partition_fields))
            self.logger.info("createHiveTableImpl [{}] - partition_fields3: {}".format(k, *createHiveTableRequestObj._partition_fields))

            new_table = createHiveTableRequestObj._db + "." + createHiveTableRequestObj._table
            check_final_table_exist = createHiveTableRequestObj._table in self.spark.getSQLContext().tableNames(
                createHiveTableRequestObj._db)

            self.logger.info("createHiveTableImpl [{}] - new_table: {}".format(k, new_table))
            self.logger.info(
                "createHiveTableImpl [{}] - check_final_table_exist: {}".format(k, check_final_table_exist))

            if not check_final_table_exist:
                self.logger.info("createHiveTableImpl [{}] - Inizio creazione tabella finale".format(k))
                df_final = self.spark.getSpark().createDataFrame(self.spark.getSparkContext().emptyRDD(),
                                                                 createHiveTableRequestObj._schema)
                if self.enableOperations:
                    df_final = df_final.write \
                        .format(createHiveTableRequestObj._file_format) \
                        .option("path", createHiveTableRequestObj._location) \
                        .mode(createHiveTableRequestObj._mode)

                    if hasattr(createHiveTableRequestObj,
                               "_partition_fields") and createHiveTableRequestObj.partition_fields() is not None:
                        self.logger.info("createHiveTableImpl [{}] - set partition_fields".format(k))
                        partition_fields = createHiveTableRequestObj._partition_fields
                        df_final = df_final.partitionBy(*partition_fields)

                    df_final.saveAsTable(new_table)
                    self.logger.info("createHiveTableImpl [{}] - Fine creazione tabella finale".format(k))

            self.logger.info("createHiveTableImpl [{}] - END".format(k))
        except Exception as e:
            self.logger.error("createHiveTableImpl [{}] - KO".format(k))
            self.manageException(e)

    #def menageWriteToHiveException(self, e, writeDataToHiveRequestObj):
    def menageWriteToHiveException(self, e):
        self.manageException(e)

    def beforeEndWriteDataToHive(self, writeDataToHiveRequestObj):
        pass

    @TimingAndProfile()
    def writeDataToHive(self, writeDataToHiveRequestList):
        threaded = self.app_writedatatohive_threaded
        return self.executeMethod(method=self.writeDataToHiveImpl, requestList=writeDataToHiveRequestList, threaded=threaded, returned=False)

    def writeDataToHiveImpl(self, writeDataToHiveRequestObj):
        try:
            k = writeDataToHiveRequestObj.key()
            self.logger.info("writeDataToHiveImpl [{}] - INIT".format(k))
            self.logger.info("writeDataToHiveImpl [{}] - table: {}".format(k, writeDataToHiveRequestObj.table()))
            self.logger.info("writeDataToHiveImpl [{}] - mode: {}".format(k, writeDataToHiveRequestObj.mode()))
            self.logger.info("writeDataToHiveImpl [{}] - location: {}".format(k, writeDataToHiveRequestObj.location()))
            self.logger.info("writeDataToHiveImpl [{}] - partition_fields: {}".format(k,
                                                                                      writeDataToHiveRequestObj.partition_fields()))
            self.logger.info("writeDataToHiveImpl [{}] - file_format: {}".format(k, writeDataToHiveRequestObj.file_format()))

            if writeDataToHiveRequestObj is None or writeDataToHiveRequestObj._data is None:
                self.logger.warning("writeDataToHiveImpl [{}] - data is None".format(k))
                self.logger.info("writeDataToHiveImpl [{}] - END".format(k))
                self.beforeEndWriteDataToHive(writeDataToHiveRequestObj)
                return

            df = writeDataToHiveRequestObj._data

            mode = writeDataToHiveRequestObj._mode
            location = writeDataToHiveRequestObj._location
            table = writeDataToHiveRequestObj._table
            file_format = writeDataToHiveRequestObj._file_format

            df = df.write \
                .format(file_format) \
                .mode(mode) \
                .option("path", location)

            #df = df.format(file_format) if file_format is not None else df

            if hasattr(writeDataToHiveRequestObj,
                       "_partition_fields") and writeDataToHiveRequestObj.partition_fields() is not None:
                self.logger.info("writeDataToHiveImpl [{}] - set partition_fields".format(k))
                partition_fields = writeDataToHiveRequestObj._partition_fields
                df = df.partitionBy(*partition_fields)

            df.saveAsTable(table) if self.enableOperations else None

            '''
            if hasattr(writeDataToHiveRequestObj,"partition_fields"):
                df.write \
                    .mode(mode) \
                    .option("path", location) \
                    .partitionBy(*partition_fields) 
                    .saveAsTable(table)
            else:
                df.write \
                    .mode(mode) \
                    .option( "path", location) \
                    .saveAsTable(table)
            '''
            self.logger.info("writeDataToHiveImpl [{}] - END".format(k))

            self.beforeEndWriteDataToHive(writeDataToHiveRequestObj)
        except Exception as e:
            self.logger.error("writeDataToHiveImpl [{}] - KO".format(k))
            self.menageWriteToHiveException(e)

    #def menageWriteToDbException(self, e, writeDataToDbRequestObj):
    def menageWriteToDbException(self, e):
        self.manageException(e)
        
    def beforeEndWriteDataToDb(self, writeDataToDbRequestObj):
        pass

    @TimingAndProfile()
    def writeDataToDb(self, writeDataToDbRequestList):
        threaded = self.app_writedatatodb_threaded
        return self.executeMethod(method=self.writeDataToDbImpl, requestList=writeDataToDbRequestList, threaded=threaded, returned=False)

    def writeDataToDbImpl(self, writeDataToDbRequestObj):
        try:
            k = writeDataToDbRequestObj.key()
            self.logger.info("writeDataToDbImpl [{}] - INIT".format(k))
            self.logger.info("writeDataToDbImpl [{}] - table: {}".format(k, writeDataToDbRequestObj.table()))
            self.logger.info("writeDataToDbImpl [{}] - mode: {}".format(k, writeDataToDbRequestObj.mode()))
            self.logger.info("writeDataToDbImpl [{}] - url: {}".format(k, writeDataToDbRequestObj.url()))
            self.logger.info("writeDataToDbImpl - properties: {}".format(writeDataToDbRequestObj.properties()))

            if writeDataToDbRequestObj._data is None:
                self.logger.warning("writeDataToDbImpl [{}] - data is None".format(k))
                return

            df = writeDataToDbRequestObj._data
            url = writeDataToDbRequestObj._url
            table = writeDataToDbRequestObj._table
            properties = writeDataToDbRequestObj._properties
            mode = writeDataToDbRequestObj._mode
            self.logger.info("writeDataToDbImpl [{}] - Inizio scrittura dati in db".format(k))
            # query_fields=",".join(fields) if fields is not None else "*"
            # df=df.select(query_fields)
            # Si potrebbero avere delle option durante la scrittura, nel caso mettere un flag con le option passate durante la buildwritetoDb
            # if writeDataToDbRequestObj._withOption==True:
            #    df.write.option(stringa).jdbc(url, table, properties=properties, mode=mode)
            # else
            df.write.jdbc(url, table, properties=properties, mode=mode) if self.enableOperations else None
            self.logger.info("writeDataToDbImpl [{}] - Fine scrittura dati in db".format(k))
            
            self.beforeEndWriteDataToDb(writeDataToDbRequestObj)
            
            self.logger.info("writeDataToDbImpl [{}] - END".format(k))
        except Exception as e:
            self.logger.error("writeDataToDbImpl [{}] - KO".format(k))
            self.menageWriteToDbException(e)

    '''
    def writeDataToOracleCx(self, writeDataToOracleRequestList):
        threaded = self.app_writedatatodb_threaded
        return self.executeMethod(self.writeDataToOracleCxImpl, writeDataToOracleRequestList, threaded, False)

    def writeDataToOracleCxImpl(self, writeDataToOracleRequestObj):
        try:

            k = writeDataToOracleRequestObj.key()
            k2 = writeDataToOracleRequestObj.key2()
            self.logger.info("writeDataToOracleCxImpl [{}__{}] - INIT".format(k,k2))
            self.logger.info("writeDataToOracleCxImpl [{}] - table: {}".format(k,writeDataToOracleRequestObj.table()))

            if writeDataToOracleRequestObj is None:
                self.logger.warning("writeDataToOracleCxImpl [{}] - data is None".format(k))
                return

            data=writeDataToOracleRequestObj.data()
            query=writeDataToOracleRequestObj.query()
            fields=writeDataToOracleRequestObj.fields()

            self.logger.info("writeDataToOracleCxImpl [{}] - Inizio scrittura in Oracle Cx".format(k))
            self.logger.info("writeDataToOracleCxImpl - query: [{}]".format(query))
            self.logger.info("writeDataToOracleCxImpl - fields: [{}]".format(fields))

            self.dbClient.executeMany(query,data,commit=True,fields=fields)

            self.logger.info("writeDataToOracleCxImpl [{}] - Fine scrittura in Oracle Cx".format(k))

            self.logger.info("writeDataToOracleCxImpl [{}] - END".format(k))
        except Exception as e:
            self.logger.error("writeDataToOracleCxImpl [{}] - KO".format(k))
            self.menageWriteToOracleException(e, writeDataToOracleRequestObj)
            #self.manageException(e, ErrorException.KEY_ORACLE, info=writeDataToOracleRequestObj)
    '''

    def endOK(self):
        self.logger.info("endOK - INIT")
        if not hasattr(self,"filename_list"): 
            self.filename_list=None
        print("endOK - INIT")
        self.logger.info("exit_code : {}".format(self.exit_code))
        print(f"endOK - FILeNAMeLIST: {self.filename_list}")
        print(f"{self.enableOperations} - {self.app_db_logger}")
        try:
            self.dbLogger.logEndExecution(self.exit_code,filename_list=self.filename_list) if (self.enableOperations and self.app_db_logger) else None
        except Exception as e:
            self.logger.error(f"logEndExecution Error {e}")
            print("logEndExecution Error", e)


        if self.dbClient:
            self.dbClient.close()
        if self.spark.getSparkContext():
            self.spark.getSparkContext().stop()

        self.logger.info("JOB EXECUTION: SUCCESS")
        if not self.exception_manager.isErrorFree():
            self.logger.info("SCRIPT EXECUTION: ERROR")
            self.logger.info(
                "NUMBER OF ERRORS: {}".format(self.exception_manager.getNumberOfExceptions()))
            self.logger.info(self.exception_manager)
        else:
            self.logger.info("SCRIPT EXECUTION: SUCCESS")

        self.logger.info("endOK - END")

        exec_time = (datetime.now() - self.script_start_time).total_seconds()
        self.logger.info("EXECUTION TIME: {} seconds".format(exec_time))
        
        # upload log in hdfs
        try:
            local_log = self.log_local_folder + "/" + self.log_filename
            remote_log = self.log_remote_folder + "/" + self.log_filename
            self.getHdfsClient().hdfsUploadAndRemove(remote_log, local_log) if self.enableOperations else None
        except Exception as e:
            self.logger.warning("endOK - WARNING - hdfs log file upload failed")

    def endKO(self, e, exit_code=None):
        print( "endOK - INIT" )
        # endKO potrebbe essere chiamata prima che venga instanziato il logger
        hasLogger = hasattr(self, "logger")
        self.logger.info("endKO - INIT") if hasLogger else print("endKO - INIT")
        self.logger.error(e) if hasLogger else print("ERROR - {}".format(e))

        if (exit_code is None):
            exit_code = self.exception_manager.getExceptionCode(code_lv1=self.app_code)

        self.logger.info("exit_code : {}".format(exit_code)) if hasLogger else print("exit_code : {}".format(exit_code))

        if self.app_db_logger and hasattr(self, "dbLogger") and self.dbLogger:
            self.dbLogger.logEndExecution(exit_code,filename_list=self.filename_list)
        if hasattr(self, "dbClient") and self.dbClient:
            self.dbClient.close()
        if hasattr(self, "spark") and self.spark.getSparkContext():
            self.spark.getSparkContext().stop()

        self.logger.info("JOB EXECUTION: ERROR")
        self.logger.info(self.exception_manager)

        self.logger.info("endKO - END") if hasLogger else print("endKO - END")

        exec_time = (datetime.now() - self.script_start_time).total_seconds()
        self.logger.info("EXECUTION TIME: {} seconds".format(exec_time)) if hasLogger else print("EXECUTION TIME: {} seconds".format(exec_time))

        if self.debug_print:
            msg_traceback = "######################### TRACEBACK {} #########################"
            self.logger.debug(msg_traceback.format("init")) if hasLogger else print(msg_traceback.format("init"))
            traceback.print_exc() if self.debug_print else None
            self.logger.debug(msg_traceback.format("end")) if hasLogger else print(msg_traceback.format("end"))
            
        # upload log in hdfs
        try:
            local_log = self.log_local_folder + "/" + self.log_filename
            remote_log = self.log_remote_folder + "/" + self.log_filename
            self.getHdfsClient().hdfsUploadAndRemove(remote_log, local_log) if self.enableOperations else None
        except Exception as e:
            self.logger.warning("endKO - WARNING - hdfs log file upload failed") if hasLogger \
                else print("endKO - WARNING - hdfs log file upload failed")

        sys.exit(1)
        #raise Exception('Error')

    @TimingAndProfile()
    def buildDataFrameMain(self, buildDataFrameRequestList):
        threaded = self.app_builddataframe_threaded
        self.logger.info("buildDataFrame - threaded = {}".format(threaded))
        return self.executeMethod(method=self.buildDataFrameImpl, requestList=buildDataFrameRequestList, threaded=threaded, returned=True)

    def buildDataFrame(self, buildDataFrameRequestObj):
        pass

    '''
    def buildDataFrameImpl(self, buildDataFrameRequestObj):
        buildDataFrameResponseObj = self.buildDataFrame(buildDataFrameRequestObj)
        buildDataFrameResponseList = self.common.toList(buildDataFrameResponseObj)
        for buildDataFrameResponse in buildDataFrameResponseList:
            if buildDataFrameResponse is not None:
                buildDataFrameResponse.key(buildDataFrameRequestObj.key())
                # buildDataFrameResponse.key2(
                    # buildDataFrameRequestObj.key() if buildDataFrameRequestObj.key2() is None else buildDataFrameRequestObj.key2())
                if buildDataFrameResponse.key2() is None:
                    buildDataFrameResponse.key2(
                        buildDataFrameRequestObj.key() if buildDataFrameRequestObj.key2() is None else buildDataFrameRequestObj.key2())
        return buildDataFrameResponseList
    '''

    def buildDataFrameImpl(self, buildDataFrameRequestObj):
        buildDataFrameResponseObj = self.buildDataFrame(buildDataFrameRequestObj)
        return self.buildDataFrameToList(buildDataFrameRequestObj, buildDataFrameResponseObj)

    def buildDataFrameToList(self, buildDataFrameRequestObj, buildDataFrameResponseObj):
        buildDataFrameResponseList = self.common.toList(buildDataFrameResponseObj)
        for buildDataFrameResponse in buildDataFrameResponseList:
            if buildDataFrameResponse is not None:
                buildDataFrameResponse.key(buildDataFrameRequestObj.key())
                # buildDataFrameResponse.key2(
                    # buildDataFrameRequestObj.key() if buildDataFrameRequestObj.key2() is None else buildDataFrameRequestObj.key2())
                if buildDataFrameResponse.key2() is None:
                    buildDataFrameResponse.key2(
                        buildDataFrameRequestObj.key() if buildDataFrameRequestObj.key2() is None else buildDataFrameRequestObj.key2())
        return buildDataFrameResponseList


    # DECORATORS
    def handle_exception(self,func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                self.manageException(e)

        return wrapper
