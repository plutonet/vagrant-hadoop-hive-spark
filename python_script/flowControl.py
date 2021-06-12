from queue import *
import threading
from dataIngestionImpl import *
import signal
from pyspark.sql.types import *

"""
Versione 1.1 
ultima Modifca : Giuseppe 17/07/2019
aggiunta: timeout ReadData

v 1.2 Antonino
- aggiunta resetRequest()
- modifica cycleRun() - aggiunta resetRequest()
- aggiunta beforeEnd_error()

v 1.3 Giuseppe 20/10/2019
- aggiunta signal SIGINT

v 1.4 Antonino 04/11/2019
- aggiunta funzione unionDataframes 04/11/2019

v.1.5 Giuseppe 19/11/2019
- aggiunta personalizzazione log_file nome dinamico (per RMT)
"""

class FlowControl(DataIngestionImpl):
    def __init__(self):
        super(FlowControl, self).__init__()

        try:
            self.readDataRequestList = []
            self.createHiveTableRequestList = []
            self.writeDataToCSVRequestList = []
            self.writeDataToHdfsRequestList = []
            self.writeDataToHiveRequestList = []
            self.writeDataToDbRequestList=[]
            self.buildDataFrameRequestList=[]

            self.initRequests()
        except Exception as e:
            self.logger.error("FlowControl.__init__ - ERROR")
            # da cancellare.. se gestite bene, a questo livello dovrebbero essere tutte IngestionException
            if not isinstance(e, IngestionException):
                traceback.print_exc() # dovresti fare la raise cosi va nell'handle_uncaught_exception()
                raise e
            self.exit_code = self.exception_manager.getExceptionCode(code_lv1=self.app_code)
            self.endKO(e, self.exit_code)

    @TimingAndProfile()
    def initRequests(self):
        cfg = self.cfg
        self.logger.debug("initRequests - config file: {}".format(cfg))
        self.custom_method = self.common.getJsonValue(cfg, "app", "custom_method", defval=False)
        self.logger.info("initRequests - custom_method: {}".format(self.custom_method))

        try:
            #GESTIONE SERVIZI
            services = self.common.getJsonValue(cfg,"service")

            #Ciclo i servizi
            key_1 = 1
            for service in services :
                #SERVICE
                #Ciclo i risultati dei servizi (rest e soap retsituiscono 1 solo risultato)
                id_service = self.common.getJsonValue(service, "id_service")
                #key = str(key_1)
                key = id_service if id_service is not None else str(key_1)
                key_2 = 1

                ## BUILD READ DATA REQUEST
                self.buildReadDataRequest(key=key, key2=key,
                  url=self.common.getJsonValue(service,"url"),
                  type=self.common.getJsonValue(service,"type"),
                  method=self.common.getJsonValue(service,"method"),
                  filename=self.common.getJsonValue(service,"filename"),
                  payload=self.common.getJsonValue(service,"payload"),
                  timeout=self.common.getJsonValue(service,"timeout"),
                  user=self.common.getJsonValue(service,"user"),
                  password=self.common.getJsonValue(service,"password"),
                  ca_location=self.common.getJsonValue(service,"ca_location"),
                  key_location=self.common.getJsonValue(service,"key_location"),
                  certificate_location=self.common.getJsonValue(service,"certificate_location"),
                  next_call=self.common.getJsonValue(service,"next_call")
                )


                ## BUILD WRITE DATA HDFS REQUEST
                # una sola scrittura per ogni service
                if self.common.checkJsonKeyExists(cfg, "raw"): # uso raw invece di 'hdfs' (hdfs potrebbe essere usato al di fuori dell'ingestion)
                    self.buildWriteDataToHdfsRequest(key=key, key2=key,
                      filename=self.common.getJsonValue(service,"raw","hdfs","filename"),
                      path=self.common.getJsonValue(service,"raw","hdfs","path")
                    )

                service_ingestions = self.common.toList(self.common.getJsonValue(service, "ingestion"))  # in realtà è già una lista
                if(len(service_ingestions) == 0):
                    # per usare la unionDataframes, nel config.json, non impostare le 'ingestion', ma metti
                    # i parametri al livello superiore (es. 'common'), altrimenti non riesce a prelevare i parametri
                    # (la unionDataframes chiama le build). Se però la union non viene usata, allora le build non verranno mai chiamate,
                    # quindi qua faccio il check e l'eventuale chiamata
                
                    ## BUILD WRITE DATA DB REQUEST
                    if self.common.checkJsonKeyExists(cfg, "db"):
                        self.buildWriteDataToDbRequest(key=key, key2=key)
                        
                    ## BUILD WRITE DATA HIVE REQUEST
                    if self.common.checkJsonKeyExists(cfg, "hive"):
                        ###### Cancellaaaa - per evitare che tenti di fare la build, senza aver imposto nessun parametro nel config
                        if self.getParam("common", "service", "ingestion", "hive") is None and len(self.getParam("hive").keys()) == 2:
                            continue
                        ######  
                            
                        if self.app_createhivetable:
                            self.buildCreateHiveTableRequest(key=key, key2=key)                            
                    
                        hive_db = self.getParam("common", "service", "ingestion", "hive", "db")
                        hive_db = hive_db if hive_db is not None else self.getParam("hive", "db")
                        assert hive_db is not None, "buildWriteDataToHiveRequest - 'db' not set in configuration file"

                        hive_table = self.getParam("common","service","ingestion","hive","table")
                        hive_table = hive_table if hive_table is not None else self.getParam("hive","table")
                        assert hive_table is not None, "buildWriteDataToHiveRequest - 'table' not set in configuration file"

                        hive_table = hive_table if ("." not in hive_table) else hive_table.split(".")[-1]
                        db_and_table = "{}.{}".format(hive_db, hive_table)

                        self.buildWriteDataToHiveRequest(key=key, key2=key, table=db_and_table)
                else:
                    for ingestion in service_ingestions:
                        filename = self.common.getJsonValue(ingestion,"filename")
                        id_ingestion = self.common.getJsonValue(ingestion, "id_ingestion")
                        key2 = id_ingestion if id_ingestion is not None else filename
                        key2 = key if key2 is None else key2

                        ## BUILD WRITE DATA DB REQUEST
                        if self.common.checkJsonKeyExists(cfg, "db"):
                            self.buildWriteDataToDbRequest(key=key, key2=key2,
                              spark=self.common.getJsonValue(ingestion,"db","spark"),
                              type=self.common.getJsonValue(ingestion,"db","type"),
                              host=self.common.getJsonValue(ingestion,"db","host"),
                              port=self.common.getJsonValue(ingestion,"db","port"),
                              service=self.common.getJsonValue(ingestion,"db","service"),
                              database=self.common.getJsonValue(ingestion,"db","database"),
                              schema=self.common.getJsonValue(ingestion,"db","schema"),
                              user=self.common.getJsonValue(ingestion,"db","user"),
                              password=self.common.getJsonValue(ingestion,"db","password"),
                              table=self.common.getJsonValue(ingestion,"db","table")
                            )

                        ## BUILD WRITE DATA HIVE REQUEST
                        if self.common.checkJsonKeyExists(cfg, "hive"):
                            ###### Cancellaaaa - per evitare che tenti di fare la build, senza aver imposto nessun parametro nel config
                            if self.getParam("common", "service", "ingestion", "hive") is None and len(self.getParam("hive").keys()) == 2:
                                continue
                            ######   
                            
                            if self.app_createhivetable:
                                self.buildCreateHiveTableRequest(key=key, key2=key2,
                                    table=self.common.getJsonValue(ingestion,"hive","table"),
                                    db=self.common.getJsonValue(ingestion,"hive","db"),
                                    schema=eval(self.common.getJsonValue(ingestion,"hive","schema")),
                                    mode=self.common.getJsonValue(ingestion,"hive","mode"),
                                    location=self.common.getJsonValue(ingestion,"hive","location"),
                                    partition_fields=eval(self.common.getJsonValue(ingestion,"hive","partitions")),
                                    file_format=self.common.getJsonValue(ingestion,"hive","file_format")
                                )

                            #
                            hive_db = self.common.getJsonValue(ingestion,"hive","db")
                            hive_db = hive_db if hive_db is not None else self.getParam("common", "service", "ingestion", "hive", "db")
                            hive_db = hive_db if hive_db is not None else self.getParam("hive", "db")
                            assert hive_db is not None, "buildWriteDataToHiveRequest - 'db' not set in configuration file"

                            hive_table = self.common.getJsonValue(ingestion,"hive","table")
                            hive_table = hive_table if hive_table is not None else self.getParam("common","service","ingestion","hive","table")
                            hive_table = hive_table if hive_table is not None else self.getParam("hive","table")
                            assert hive_table is not None, "buildWriteDataToHiveRequest - 'table' not set in configuration file"

                            hive_table = hive_table if ("." not in hive_table) else hive_table.split(".")[-1]
                            db_and_table = "{}.{}".format(hive_db, hive_table)
                            #db_and_table = "{}.{}".format(hive_db, hive_table) if ("." not in hive_table) else hive_table
                            #
                            self.buildWriteDataToHiveRequest(key=key, key2=key2,
                                mode=self.common.getJsonValue(ingestion,"hive","mode"),
                                location=self.common.getJsonValue(ingestion,"hive","location"),
                                table=db_and_table,
                                partition_fields=eval(self.common.getJsonValue(ingestion,"hive","partitions")),
                                schema=eval(self.common.getJsonValue(ingestion,"hive","schema"))
                            )

                        key_2 += 1
                key_1 += 1
        except Exception as e:
            self.logger.error("initRequests - ERROR")
            raise self.manageException(e)


    def endTimeout(self,sig, frame):
        exit_code=124                                  
        self.logger.info("TIMEOUT")
        if self.dbLogger:
            self.dbLogger.logEndExecution(exit_code)
        if self.db:
            self.db.close()
        if self.spark.getSparkContext():
            self.spark.getSparkContext().stop()
        if self.logger:
            self.logger.__del__()
        raise Exception("TIMEOUT EXPIRED")
    
    # Reset oggetti di flusso
    def resetRequest (self) :
        self.readDataRequestList.clear()
        self.writeDataToHdfsRequestList.clear()
        self.createHiveTableRequestList.clear()
        self.writeDataToCSVRequestList.clear()
        self.writeDataToHiveRequestList.clear()
        self.writeDataToDbRequestList.clear()

    # Costruzione oggetti di flusso
    # Servizio http
    def buildReadDataRequest(self, key=None,key2=None, url=None, type=None, method=None, filename=None, payload=None,timeout=None,user=None,password=None,ca_location=None,key_location=None,certificate_location=None,next_call=None):
        key = key if key is not None else -1
        key2 = key2 if key2 is not None else key

        url = url if url is not None else self.getParam("common","service","url")
        assert url is not None, "buildReadDataRequest - url not set in configuration file"

        user = user if user is not None else self.getParam("common","service","user")
        password = password if password is not None else self.getParam("common","service","password")

        type = type if type is not None else self.getParam("common","service","type")
        assert type is not None, "buildReadDataRequest - type not set in configuration file"

        method = method if method is not None else self.getParam("common","service","method")
        #assert method is not None, "buildReadDataRequest - method not set in configuration file"

        filename = filename if filename is not None else self.getParam("common","service","filename")
        payload = payload if payload is not None else self.getParam("common","service","payload")
        timeout = timeout if timeout is not None else self.getParam("common","service","timeout")

        ca_location = ca_location if ca_location is not None else self.getParam("common","service","ca_location")
        key_location = key_location if key_location is not None else self.getParam("common","service","")
        certificate_location = certificate_location if certificate_location is not None else self.getParam("common","service","certificate_location")
        
        next_call = next_call if next_call else self.getParam("common", "service", "next_call")

        ## istanzio ftp/ssh client (al momento solo qua posso farlo) -> UPDATE: qua ti puoi settare le variabili self che
        # poi vengono usate da getFtpClient e getSshClient
        '''
        protocol = url.split("://")[0]
        if protocol.upper() == ProtocolType.SSH:
            path = url[len(protocol) + len("://"):]
            host_port = path.split("/")[0]
            host = host_port.split(":")[0]
            port = host_port.split(":")[1]
            self.getSshClient(host, user, password, port)
        elif protocol.upper() == ProtocolType.FTP:
            path = url[len(protocol) + len("://"):]
            host_port = path.split("/")[0]
            host = host_port.split(":")[0]
            self.getFtpClient(host, user, password)
        elif protocol.upper() == ProtocolType.KAFKA:
            path = url[len(protocol) + len("://"):]
            host_port = path.split("/")[0]
            host = host_port.split(":")[0]
            topic = host_port.split("/")[1]
            group_id = host_port.split("/")[2]
        '''
        ####

        self.logger.debug("key: {}".format(key))
        self.logger.debug("key2: {}".format(key2))
        self.logger.debug("url: {}".format(url))
        self.logger.debug("user: {}".format(user))
        self.logger.debug("password: {}".format(password))
        self.logger.debug("type: {}".format(type))
        self.logger.debug("filename: {}".format(filename))
        self.logger.debug("method: {}".format(method))
        self.logger.debug("payload: {}".format(payload))
        self.logger.debug("timeout: {}".format(timeout))
        self.logger.debug("ca_location: {}".format(ca_location))
        self.logger.debug("key_location: {}".format(key_location))
        self.logger.debug("certificate_location: {}".format(certificate_location))
        self.logger.debug("next_call: {}".format(next_call))

        readDataFromRequestObj = ReadDataRequest()\
            .key(key)\
            .key2(key2)\
            .url(url)\
            .user(user)\
            .password(password)\
            .type(type)\
            .filename(filename)\
            .method(method) \
            .payload(payload) \
            .timeout(timeout) \
            .ca_location(ca_location) \
            .key_location(key_location)\
            .certificate_location(certificate_location)\
            .next_call(next_call)
        self.readDataRequestList.append(readDataFromRequestObj)

    # Servizio scrittura json in hdfs
    def buildWriteDataToHdfsRequest(self, key=None, key2=None, filename=None, path=None):
        key = key if key is not None else -1
        key2 = key2 if key2 is not None else key

        path = path if path is not None else self.getParam("common","service","raw","hdfs","path")
        assert path is not None, "buildWriteDataToHdfsRequest - path not set in configuration file"

        filename = filename if filename is not None else self.getParam("common","service","raw","hdfs","filename")
        #assert filename is not None, "buildWriteDataToHdfsRequest - filename not set in configuration file"

        self.logger.debug("key: {}".format(key))
        self.logger.debug("key2: {}".format(key2))
        self.logger.debug("filename: {}".format(filename))
        self.logger.debug("path: {}".format(path))

        writeDataToHdfsRequestObj = WriteDataToHdfsRequest() \
            .key(key) \
            .key2(key2) \
            .filename(filename)\
            .path(path)
        self.writeDataToHdfsRequestList.append(writeDataToHdfsRequestObj)
        

     # Servizio scrittura csv in hdfs via spark
    def buildWriteDataToCsvRequest(self,key=None,key2=None, filename=None,path=None,separator=',',hasHeader=True):
        if hasattr(self,"csv_file_dict"):
            for k, v in self.csv_file_dict.items():
                writeDataToCSVRequestObj = WriteDataToCSVRequest() \
                    .key(key) \
                    .key2( k) \
                    .filename( v["hdfs_filename_csv"] ) \
                    .path( v["hdfs_path_csv"]  ) \
                    .separator( v["hdfs_separator_csv"]) \
                    .hasHeader( v["hdfs_hasheader_csv"] )
                self.writeDataToCSVRequestList.append( writeDataToCSVRequestObj )
        else:
            writeDataToCSVRequestObj = WriteDataToCSVRequest() \
                        .key(key) \
                        .key2(key2)\
                        .filename(filename)\
                        .path(path)\
                        .separator(separator)\
                        .hasHeader(hasHeader)
            self.writeDataToCSVRequestList.append(writeDataToCSVRequestObj)

    # Servizio scrittura in hive via spark
    def buildWriteDataToHiveRequest(self,key=None, key2=None, mode=None, location=None, table=None, partition_fields=None, schema=None, file_format=None):
        key = key if key is not None else -1
        key2 = key2 if key2 is not None else key
        
        mode = mode if mode is not None else self.getParam("common","service","ingestion","hive","mode")
        mode = mode if mode is not None else self.getParam("hive","mode")
        assert mode is not None, "buildWriteDataToHiveRequest - 'mode' not set in configuration file"

        location = location if location is not None else self.getParam("common","service","ingestion","hive","location")
        location = location if location is not None else self.getParam("hive","location")
        assert location is not None, "buildWriteDataToHiveRequest - 'location' not set in configuration file"

        '''
        if table is None:
            db_only = self.getParam("common", "service", "ingestion", "hive", "db")
            db_only = db_only if db_only is not None else self.getParam("hive", "db")
            assert db_only is not None, "buildWriteDataToHiveRequest - 'db' not set in configuration file"

            table_only = self.getParam("common","service","ingestion","hive","table")
            table_only = table_only if table_only is not None else self.getParam("hive","table")
            assert table_only is not None, "buildWriteDataToHiveRequest - 'table' not set in configuration file"

            table = "{}.{}".format(db_only, table_only)
        '''

        partition_fields = partition_fields if partition_fields is not None else self.getParam("common","service","ingestion","hive","partitions")
        partition_fields = partition_fields if partition_fields is not None else self.getParam("hive","partitions")
        assert partition_fields is not None, "buildWriteDataToHiveRequest - 'partitions' not set in configuration file"

        schema = schema if schema is not None else self.getParam("common","service","ingestion","hive","schema")
        schema = schema if schema is not None else self.getParam("hive","schema")
        assert schema is not None, "buildWriteDataToHiveRequest - 'schema' not set in configuration file"
         
        file_format = file_format if file_format is not None else self.getParam("common","service","ingestion","hive","file_format")
        file_format = file_format if file_format is not None else self.getParam("hive","file_format")
        if file_format is None or file_format.lower() not in HiveFileFormat.__members__.values():
            file_format = HiveFileFormat.ORC
        else:
            file_format = file_format.lower()

        #assert format is not None, "buildWriteDataToHiveRequest - format not set in configuration file"

        self.logger.debug("key: {}".format(key))
        self.logger.debug("key2: {}".format(key2))
        self.logger.debug("mode: {}".format(mode))
        self.logger.debug("location: {}".format(location))
        self.logger.debug("table: {}".format(table))
        self.logger.debug("partition_fields: {}".format(partition_fields))
        self.logger.debug("schema: {}".format(schema))
        self.logger.debug("file_format: {}".format(file_format))
        
        ######
        if hasattr( self, "hive_table_dict" ):
            for k,v in self.hive_table_dict.items():
                writeDataToHiveRequestObj = WriteDataToHiveRequest() \
                            .key(key) \
                            .key2( k ) \
                            .mode(v["hive_final_mode"])\
                            .location(v["hive_final_location"])\
                            .table(v["hive_final_db_and_table"]) \
                            .partition_fields(v["hive_final_partitions"]) 
                            #.format(v["hive_final_format"] if "hive_final_format" in v else format   )
                self.writeDataToHiveRequestList.append( writeDataToHiveRequestObj )
        else:
            ''' cancella
            writeDataToHiveRequestObj = WriteDataToHiveRequest() \
                .key( self.getparamold( key, "app_key" ) ) \
                .key2( self.getparamold( key2, "app_key" ) ) \
                .mode( self.getparamold( mode, "hive_final_mode" ) ) \
                .location( self.getparamold( location, "hive_final_location" ) ) \
                .table( self.getparamold( table, "hive_final_db_and_table" ) ) \
                .partition_fields( self.getparamold( partition_fields, "hive_final_partitions")) \
                .file_format(self.getparamold( file_format, "hive_file_format" ))
            '''
            writeDataToHiveRequestObj = WriteDataToHiveRequest() \
                .key(key) \
                .key2(key2) \
                .mode(mode) \
                .location(location) \
                .table(table) \
                .partition_fields(partition_fields) \
                .file_format(file_format) \
                .schema(schema)

            self.writeDataToHiveRequestList.append( writeDataToHiveRequestObj )


    def buildCreateHiveTableRequest(self,key=None,key2=None,table=None, db=None, schema=None, mode=None,location=None,partition_fields=None,file_format=None):
        key = key if key is not None else -1

        db = db if db is not None else self.getParam("common", "service", "ingestion", "hive", "db")
        db = db if db is not None else self.getParam("hive", "db")
        assert db is not None, "buildCreateHiveTableRequest - 'db' not set in configuration file"

        table = table if table is not None else self.getParam("common", "service", "ingestion", "hive", "table")
        table = table if table is not None else self.getParam("hive", "table")
        assert table is not None, "buildCreateHiveTableRequest - 'table' not set in configuration file"

        mode = mode if mode is not None else self.getParam("common", "service", "ingestion", "hive", "mode")
        mode = mode if mode is not None else self.getParam("hive", "mode")
        assert mode is not None, "buildCreateHiveTableRequest - 'mode' not set in configuration file"

        location = location if location is not None else self.getParam("common", "service", "ingestion", "hive",
                                                                       "location")
        location = location if location is not None else self.getParam("hive", "location")
        assert location is not None, "buildCreateHiveTableRequest - 'location' not set in configuration file"

        partition_fields = partition_fields if partition_fields is not None else self.getParam("common", "service",
                                                                                               "ingestion", "hive",
                                                                                               "partitions")
        partition_fields = partition_fields if partition_fields is not None else self.getParam("hive", "partitions")
        assert partition_fields is not None, "buildCreateHiveTableRequest - 'partitions' not set in configuration file"

        schema = schema if schema is not None else self.getParam("common", "service", "ingestion", "hive", "schema")
        schema = schema if schema is not None else self.getParam("hive", "schema")
        assert schema is not None, "buildCreateHiveTableRequest - 'schema' not set in configuration file"

        file_format = file_format if file_format is not None else self.getParam("common", "service", "ingestion", "hive", "file_format")
        file_format = file_format if file_format is not None else self.getParam("hive", "file_format")
        if file_format is None or file_format.lower() not in HiveFileFormat.__members__.values():
            file_format = HiveFileFormat.ORC
        else:
            file_format = file_format.lower()

        self.logger.debug("key: {}".format(key))
        self.logger.debug("mode: {}".format(mode))
        self.logger.debug("location: {}".format(location))
        self.logger.debug("db: {}".format(db))
        self.logger.debug("table: {}".format(table))
        self.logger.debug("partition_fields: {}".format(partition_fields))
        self.logger.debug("schema: {}".format(schema))
        self.logger.debug("file_format: {}".format(file_format))

        createHiveTableRequestObj=CreateHiveTableRequest() \
                    .key(key) \
                    .table(table)\
                    .db(db)\
                    .schema(schema)\
                    .mode(mode)\
                    .location(location)\
                    .partition_fields(partition_fields)\
                    .file_format(file_format)

        self.createHiveTableRequestList.append(createHiveTableRequestObj)


    def buildWriteDataToDbRequest(self,key=None,key2=None,spark=False,type=None,host=None,port=None,service=None,database=None,schema=None,user=None,password=None,table=None,mode=None):
        key = key if key is not None else -1
        key2 = key2 if key2 is not None else key

        spark = spark if spark else self.getParam("common","service","ingestion","db","spark")
        assert spark is not None, "buildWriteDataToDbRequest - spark not set in configuration file"

        mode = mode if mode is not None else self.getParam("common","service","ingestion","db","mode")
        assert mode is not None, "buildWriteDataToDbRequest - 'mode' not set in configuration file"

        type = type if type is not None else self.getParam("common","service","ingestion","db","type")
        type = type if type is not None else self.getParam("db","type")
        assert type is not None, "buildWriteDataToDbRequest - 'type' not set in configuration file"

        host = host if host is not None else self.getParam("common","service","ingestion","db","host")
        host = host if host is not None else self.getParam("db","host")
        assert host is not None, "buildWriteDataToDbRequest - 'host' not set in configuration file"

        port = port if port is not None else self.getParam("common","service","ingestion","db","port")
        port = port if port is not None else self.getParam("db","port")
        assert port is not None, "buildWriteDataToDbRequest - 'port' not set in configuration file"

        service = service if service is not None else self.getParam("common","service","ingestion","db","service")
        service = service if service is not None else self.getParam("db","service")
        #assert service is not None, "buildWriteDataToDbRequest - service not set in configuration file"

        database = database if database is not None else self.getParam("common","service","ingestion","db","database")
        database = database if database is not None else self.getParam("db","database")
        #assert database is not None, "buildWriteDataToDbRequest - database not set in configuration file"

        user = user if user is not None else self.getParam("common","service","ingestion","db","user")
        user = user if user is not None else self.getParam("db","user")
        assert user is not None, "buildWriteDataToDbRequest - 'user' not set in configuration file"

        password = password if password is not None else self.getParam("common","service","ingestion","db","password")
        password = password if password is not None else self.getParam("db","password")
        assert password is not None, "buildWriteDataToDbRequest - 'password' not set in configuration file"

        schema = schema if schema is not None else self.getParam("common", "service", "ingestion", "db", "schema")
        schema = schema if schema is not None else self.getParam("db", "schema")
        assert schema is not None, "buildWriteDataToDbRequest - 'schema' not set in configuration file"

        table = table if table is not None else self.getParam("common","service","ingestion","db","table")
        table = table if table is not None else self.getParam("db","table")
        assert table is not None, "buildWriteDataToDbRequest - 'table' not set in configuration file"

        table = table if ("." not in table) else table.split(".")[-1]
        schema_and_table = "{}.{}".format(schema, table)

        self.logger.debug("key: {}".format(key))
        self.logger.debug("key2: {}".format(key2))
        self.logger.debug("spark: {}".format(spark))
        self.logger.debug("type: {}".format(type))
        self.logger.debug("host: {}".format(host))
        self.logger.debug("mode: {}".format(mode))
        self.logger.debug("port: {}".format(port))
        self.logger.debug("service: {}".format(service))
        self.logger.debug("database: {}".format(database))
        self.logger.debug("schema: {}".format(schema))
        self.logger.debug("user: {}".format(user))
        self.logger.debug("password: {}".format(password))
        self.logger.debug("table: {}".format(table))

        writeDataToDbRequestObj = None
        if spark :
            driver, url, properties = self.getDbParam(\
                type = type,\
                host = host,\
                port = port,\
                service = service,\
                database = database, \
                schema = schema, \
                user = user,\
                password = password\
            )

            self.logger.info("buildWriteDataToDbRequest driver = {}".format(driver))
            self.logger.info("buildWriteDataToDbRequest url = {}".format(url))
            self.logger.info("buildWriteDataToDbRequest properties = {}".format(properties))


            writeDataToDbRequestObj = WriteDataToDbSparkRequest() \
                .key(key) \
                .key2(key2) \
                .url(url) \
                .properties(properties) \
                .mode(mode) \
                .table(schema_and_table)
        if writeDataToDbRequestObj is not None :
            self.writeDataToDbRequestList.append(writeDataToDbRequestObj)
            self.logger.debug("writeDataToDbRequestObj appended")
            self.logger.debug("writeDataToDbRequestList length: ", len(self.writeDataToDbRequestList))
    
    '''
    def buildWriteDataToOracleRequest(self,key=None,key2=None, table=None,url=None,properties=None, mode=None,cxOracle=False,query=None,fields=None):
        if hasattr(self,"oracle_table_dict"):
            for k,v in self.oracle_table_dict.items():
                writeDataToOracleRequestObj = None
                if v["cxOracle"]:
                    writeDataToOracleRequestObj = WriteDataToOracleCxRequest() \
                    .query(v["query"]) \
                    .fields(v["fields"])
                else :
                    writeDataToOracleRequestObj = WriteDataToOracleSparkRequest() \
                    .url( self.getparamold( url, "db_url" ) ) \
                    .properties( self.getparamold( properties, "db_properties" ) ) \
                    .mode( v["mode_write_oracle"] )

                writeDataToOracleRequestObj \
                    .key( self.getparamold( key, "app_key" ) ) \
                    .key2( k ) \
                    .table( v["oracle_final_table"] )
                self.writeDataToOracleRequestList.append( writeDataToOracleRequestObj )
        else:

            if cxOracle:
                writeDataToOracleRequestObj = WriteDataToOracleCxRequest() \
                    .query(self.getparamold( query, "query_oracle" ))\
                    .fields(self.getparamold( fields, "fields" ))
            else:
                writeDataToOracleRequestObj = WriteDataToOracleSparkRequest() \
                    .url( self.getparamold( url, "db_url" ) ) \
                    .properties( self.getparamold( properties, "db_properties" ) ) \
                    .mode( self.getparamold(mode, "mode_write_oracle" ))
            writeDataToOracleRequestObj \
                .key( self.getparamold( key, "app_key" ) ) \
                .key2( self.getparamold( key, "app_key" ) ) \
                .table(self.getparamold( table, "oracle_final_table" ) )

            self.writeDataToOracleRequestList.append(writeDataToOracleRequestObj)
    '''

    def searchReadDataRequest(self, key):
        for obj in self.readDataRequestList:
            if obj.key() == key:
                return obj
        return None

    def isValid(self,attr):
        return hasattr( self, attr) \
            and getattr(self,attr) is not None

    #metodo eseguito alla fine del processo
    def beforeEnd(self):
        pass
        
    #metodo eseguito alla fine del processo, in caso di errore
    def beforeEnd_error(self):
        pass

    @TimingAndProfile()
    def unionDataframes(self, buildDataFrameResponseList, limit_df_size=None):
        self.logger.info("unionDataframes [{}] - Init".format(self.app_name))
        union_df_hive, union_df_db, union_df_csv = None, None, None
        union_df_hive_list, union_df_db_list, union_df_csv_list = [], [], []

        try:
            # effetto la union dei dataframe (a seconda del parametro limit_df_size si possono avere più df union)
            count_response = 1
            for responseList in buildDataFrameResponseList:
                for buildDataFrameResponse in responseList:
                    check_last_response = (count_response == len(buildDataFrameResponseList))
                    df_hive = buildDataFrameResponse.df_hive()
                    df_db = buildDataFrameResponse.df_db()
                    df_csv = buildDataFrameResponse.df_csv()

                    if df_hive is not None:
                        union_df_hive = union_df_hive.union(df_hive) if union_df_hive is not None else df_hive
                        if limit_df_size is not None and (union_df_hive.count() > limit_df_size or check_last_response):
                            union_df_hive_list.append(union_df_hive)
                            union_df_hive = None
                    if df_db is not None:
                        union_df_db = union_df_db.union(df_db) if union_df_db is not None else df_db
                        if limit_df_size is not None and (union_df_db.count() > limit_df_size or check_last_response):
                            union_df_db_list.append(union_df_db)
                            union_df_db = None
                    if df_csv is not None:
                        union_df_csv = union_df_csv.union(df_csv) if union_df_csv is not None else df_csv
                        if limit_df_size is not None and (union_df_csv.count() > limit_df_size or check_last_response):
                            union_df_csv_list.append(union_df_csv)
                            union_df_csv = None
                    count_response += 1

            if limit_df_size is None:
                union_df_hive_list.append(union_df_hive) if union_df_hive is not None else None
                union_df_db_list.append(union_df_db) if union_df_db is not None else None
                union_df_csv_list.append(union_df_csv) if union_df_csv is not None else None

            # creo le request per ogni df risultante dalle union
            if len(union_df_hive_list) > 0:
                self.logger.info("unionDataframes [{}] - total union_df_hive: {}".format(self.app_name, len(union_df_hive_list)))
                self.writeDataToHiveRequestList = []
                for df in union_df_hive_list:
                    self.logger.info("unionDataframes [{}] - union_df_hive count: {}".format(self.app_name, df.count()))
                    # devo farlo per prelevare i parametri dal config (tabella, ecc)
                    self.buildWriteDataToHiveRequest()
                    self.writeDataToHiveRequestList[-1] = self.writeDataToHiveRequestList[-1].data(df) \
                                        .key("union_{}".format(id(df)))

            if len(union_df_db_list) > 0:
                self.logger.info("unionDataframes [{}] - total union_df_db: {}".format(self.app_name, len(union_df_db_list)))
                self.writeDataToDbRequestList = []
                for df in union_df_db_list:
                    self.logger.info("unionDataframes [{}] - union_df_db count: {}".format(self.app_name, df.count()))
                    self.buildWriteDataToDbRequest()
                    self.logger.debug("00000000000000000000000000000000000000  {}".format(len(self.writeDataToDbRequestList)))
                    self.writeDataToDbRequestList[-1] = self.writeDataToDbRequestList[-1].data(df) \
                                        .key("union_{}".format(id(df)))

            if len(union_df_csv_list) > 0:
                self.logger.info("unionDataframes [{}] - total union_df_csv: {}".format(self.app_name, len(union_df_csv_list)))
                self.writeDataToCSVRequestList = []
                for df in union_df_csv_list:
                    self.logger.info("unionDataframes [{}] - union_df_csv count: {}".format(self.app_name, df.count()))
                    self.buildWriteDataToCsvRequest()
                    self.writeDataToCSVRequestList[-1] = self.writeDataToCSVRequestList[-1].data(df) \
                                        .key("union_{}".format(id(df)))

            self.logger.info("unionDataframes [{}] - End".format(self.app_name))
        except Exception as e:
            self.logger.error("unionDataframes [{}] - ERROR".format(self.app_name))
            self.manageException(e)

    ## INIZIO GESTIONE METODI CUSTOM ##
    def readDataImplCustom(self, readDataRequestObj):
        self.logger.info("flowControl - readDataImplCustom [{}]".format(self.app_name))
        return super(FlowControl, self).readDataImpl(readDataRequestObj)

    def readDataImpl(self, readDataRequestObj):
        self.logger.info("flowControl - readDataImpl [{}]".format(self.app_name))
        if self.custom_method :
            return self.readDataImplCustom(readDataRequestObj)
        else :
            return super(FlowControl, self).readDataImpl(readDataRequestObj)

    def writeDataToHdfsImplCustom(self, writeDataToHdfsRequestObj):
        self.logger.info("flowControl - writeDataToHdfsImplCustom [{}]".format(self.app_name))
        return super(FlowControl, self).writeDataToHdfsImpl(writeDataToHdfsRequestObj)

    def writeDataToHdfsImpl(self, writeDataToHdfsRequestObj):
        self.logger.info("flowControl - writeDataToHdfsImpl [{}]".format(self.app_name))
        if self.custom_method :
            return self.writeDataToHdfsImplCustom(writeDataToHdfsRequestObj)
        else :
            return super(FlowControl, self).writeDataToHdfsImpl(writeDataToHdfsRequestObj)

    def createHiveTableImplCustom(self, createHiveTableRequestObj):
        self.logger.info("flowControl - createHiveTableImplCustom [{}]".format(self.app_name))
        return super(FlowControl, self).createHiveTableImpl(createHiveTableRequestObj)

    def createHiveTableImpl(self, createHiveTableRequestObj):
        self.logger.info("flowControl - createHiveTableImpl [{}]".format(self.app_name))
        if self.custom_method :
            return self.createHiveTableImplCustom(createHiveTableRequestObj)
        else :
            return super(FlowControl, self).createHiveTableImpl(createHiveTableRequestObj)

    def writeDataToHiveImplCustom(self, writeDataToHiveRequestObj):
        self.logger.info("flowControl - writeDataToHiveImplCustom [{}]".format(self.app_name))
        return super(FlowControl, self).writeDataToHiveImpl(writeDataToHiveRequestObj)

    def writeDataToHiveImpl(self, writeDataToHiveRequestObj):
        self.logger.info("flowControl - writeDataToHiveImpl [{}]".format(self.app_name))
        if self.custom_method :
            return self.writeDataToHiveImplCustom(writeDataToHiveRequestObj)
        else :
            return super(FlowControl, self).writeDataToHiveImpl(writeDataToHiveRequestObj)

    def writeDataToDbImplCustom(self, writeDataToDbRequestObj):
        self.logger.info("flowControl - writeDataToDbImplCustom [{}]".format(self.app_name))
        return super(FlowControl, self).writeDataToDbImpl(writeDataToDbRequestObj)

    def writeDataToDbImpl(self, writeDataToDbRequestObj):
        self.logger.info("flowControl - writeDataToDbImpl [{}]".format(self.app_name))
        if self.custom_method :
            return self.writeDataToDbImplCustom(writeDataToDbRequestObj)
        else :
            return super(FlowControl, self).writeDataToDbImpl(writeDataToDbRequestObj)

    def buildDataFrameCustom(self, buildDataFrameRequestObj):
        self.logger.info("flowControl - buildDataFrameImplCustom [{}]".format(self.app_name))
        return super(FlowControl, self).buildDataFrameImpl(buildDataFrameRequestObj)

    def buildDataFrameImpl(self, buildDataFrameRequestObj):
        self.logger.info("flowControl - buildDataFrameImpl [{}]".format(self.app_name))
        if self.custom_method :
            buildDataFrameResponseObj = self.buildDataFrameCustom(buildDataFrameRequestObj)
            return self.buildDataFrameToList(buildDataFrameRequestObj, buildDataFrameResponseObj)
        else :
            return super(FlowControl, self).buildDataFrameImpl(buildDataFrameRequestObj)

    ## FINE INIZIO GESTIONE METODI CUSTOM ##


    ## RUN ##
    def run(self):
        try:
            #if self.isValid("readDataFromRequestObj"):
            print("**************** INIT *******************")
            readDataResponseList = []
            readDataResponseListSplit = [] #split readDataResponse per risposte multiple (Es file ZIP)

            self.logger.info("self.readDataRequestList size = {} ".format(len(self.readDataRequestList)))
            if len(self.readDataRequestList) > 0:
                readDataResponseList = self.readData(self.readDataRequestList) #stringa
 
            for readDataResponse in readDataResponseList:
                readDataResponseListSplit.extend(self.readDataSplit(readDataResponse))

            self.logger.info("readDataResponseList size = {} ".format(len(readDataResponseList)))
            self.logger.info("readDataResponseList filenames = {} ".format([t.filename() for t in readDataResponseList]))
            self.logger.info("readDataResponseList is none : {}".format([t.data() is None for t in readDataResponseList]))
            self.logger.info("readDataResponseListSplit size = {} ".format(len(readDataResponseListSplit)))
            self.logger.info("writeDataToHdfsRequestList size = {} ".format(len(self.writeDataToHdfsRequestList)))

            #if self.isValid("writeDataToHdfsRequestObj"):
            if len(self.writeDataToHdfsRequestList) > 0:
                #accoppio data di response readData con request writeDataToHdfs attraverso la key
                for readDataResponse in readDataResponseList:
                    if readDataResponse.exit_code() == 0 :
                        for writeDataToHdfsRequest in self.writeDataToHdfsRequestList:
                            if readDataResponse.key() == writeDataToHdfsRequest.key():
                                hdfsDataList = self.common.toList(readDataResponse.data())
                                if writeDataToHdfsRequest.dataList() is None :
                                    writeDataToHdfsRequest.dataList(hdfsDataList)
                                else :
                                    writeDataToHdfsRequest.dataList().extend(hdfsDataList)

                                #self.logger.info("writeDataToHdfsRequest.key(): {} ".format(writeDataToHdfsRequest.key()))
                                #writeDataToHdfsRequest.data(readDataResponse.data())
                                if writeDataToHdfsRequest.filename() is None :
                                    writeDataToHdfsRequest.filename(readDataResponse.filename()) #filename originale estratto dal servizio
                    else :
                        readDataResponseList.remove(readDataResponse)
                        
                self.writeDataToHdfs(self.writeDataToHdfsRequestList)

            #if self.isValid("createHiveTableRequestObj"):
            if len(self.createHiveTableRequestList) > 0:
                self.createHiveTable(self.createHiveTableRequestList)
                
            readDataResponseList = readDataResponseListSplit
            
            print("len self.writeDataToHiveRequestList = ",len(self.writeDataToHiveRequestList))
            print("len self.writeDataToDbRequestList = ",len(self.writeDataToDbRequestList))

            if len(readDataResponseList) > 0 or len(self.writeDataToHiveRequestList) > 0 or len(self.writeDataToDbRequestList) > 0:
                if len(readDataResponseList) > 0:
                    for readDataResponse in readDataResponseList :
                        buildDataFrameRequest = BuildDataFrameRequest()\
                            .key(readDataResponse.key())\
                            .key2(readDataResponse.key2())\
                            .data(readDataResponse.data())\
                            .obj(readDataResponse.obj())
                        self.buildDataFrameRequestList.append(buildDataFrameRequest)
                    buildDataFrameResponseList = self.buildDataFrameMain(self.buildDataFrameRequestList)
                else:
                    buildDataFrameRequest = BuildDataFrameRequest()\
                        .key("-1")\
                        .data(None)\
                        .obj(None)
                    self.buildDataFrameRequestList.append(buildDataFrameRequest)
                    buildDataFrameResponseList = self.buildDataFrameMain(self.buildDataFrameRequestList)

                if self.app_builddataframe_union:
                    limit_df = None if int(self.app_builddataframe_union_limit_df_size) == -1 else int(self.app_builddataframe_union_limit_df_size)
                    self.unionDataframes(buildDataFrameResponseList, limit_df)
                else:
                    for buildDataFrameResponse in buildDataFrameResponseList:
                        #la response e' sempre una lista (di 1 o piu' elementi)
                        buildDataFrameResponseList2 = buildDataFrameResponse
                        for buildDataFrameResponse2 in buildDataFrameResponseList2:
                            if buildDataFrameResponse2 is not None :
                                #accoppio df_csv di response DataFrame con request writeDataToCsv attraverso la key
                                if buildDataFrameResponse2.df_csv() is not None :
                                    for writeDataToCSVRequest in self.writeDataToCSVRequestList:
                                        if writeDataToCSVRequest.key() == buildDataFrameResponse2.key()\
                                                and writeDataToCSVRequest.key2()== buildDataFrameResponse2.key2():                                           
                                            writeDataToCSVRequest.data( buildDataFrameResponse2.df_csv() )

                                #accoppio df_hive di response DataFrame con request writeDataToHive attraverso la key
                                if buildDataFrameResponse2.df_hive() is not None :
                                    for writeDataToHiveRequest in self.writeDataToHiveRequestList:
                                        if writeDataToHiveRequest.key() == buildDataFrameResponse2.key()\
                                                and writeDataToHiveRequest.key2() == buildDataFrameResponse2.key2():
                                            #quando match dovrebbe fare break
                                            if writeDataToHiveRequest.data():
                                                df_hive_to_write = writeDataToHiveRequest.data()
                                                df_hive_to_write = df_hive_to_write.union(buildDataFrameResponse2.df_hive())
                                            else:
                                                df_hive_to_write = buildDataFrameResponse2.df_hive()
                                            writeDataToHiveRequest.data( df_hive_to_write )
                                #accoppio df_db di response DataFrame con request writeDataToDb attraverso la key
                                if buildDataFrameResponse2.df_db() is not None :
                                    for writeDataToDbRequest in self.writeDataToDbRequestList:
                                        if writeDataToDbRequest.key() == buildDataFrameResponse2.key()\
                                                and writeDataToDbRequest.key2()== buildDataFrameResponse2.key2():
                                            if writeDataToDbRequest.data() is None :
                                                df_to_write = buildDataFrameResponse2.df_db()
                                            else :
                                                df_to_write = writeDataToDbRequest.data()
                                                df_to_write = df_to_write.union(buildDataFrameResponse2.df_db())
                                                
                                            writeDataToDbRequest.data( df_to_write )

            #scrittura in CSV
            if len(self.writeDataToCSVRequestList) > 0:
                self.writeDataToCSV(self.writeDataToCSVRequestList)

            #scrittura in Hive
            if len(self.writeDataToHiveRequestList) > 0:
                #for writeDataToHiveRequestObj in self.writeDataToHiveRequestList:
                self.writeDataToHive(self.writeDataToHiveRequestList)

            #scrittura in Db
            if len(self.writeDataToDbRequestList) > 0:
                #for writeDataToDbRequestObj in self.writeDataToDbRequestList:
                self.writeDataToDb(self.writeDataToDbRequestList)

            self.beforeEnd()
            
        except Exception as e:
            # da cancellare.. se gestite bene, a questo livello dovrebbero essere tutte IngestionException
            if not isinstance(e, IngestionException):
                print(e)
            self.exit_code = self.exception_manager.getExceptionCode(code_lv1=self.app_code)
            self.beforeEnd_error()
            self.endKO(e, self.exit_code)
        else:
            #if self.is_error_in_list:
                #self.exit_code = 233
                #self.beforeEnd_error()
                #e=Exception("Error in single Object in List")
                #self.endKO(e,self.exit_code)
                #exit()
            self.endOK()