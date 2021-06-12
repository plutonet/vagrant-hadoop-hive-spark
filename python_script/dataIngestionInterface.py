#svil
import abc

class DataIngestionInterface( abc.ABC ):
    @abc.abstractmethod
    def buildDataFrame(self, buildDataFrameRequestList): pass
    @abc.abstractmethod
    def writeDataToHdfs(self,writeJsonToHdfsRequest): pass
    @abc.abstractmethod
    def writeDataToCSV(self,writeToCSVRequest): pass
    @abc.abstractmethod
    def writeDataToHive(self,writeInHiveRequest): pass
    @abc.abstractmethod
    def writeDataToDb(self,writeInDbRequest): pass
    @abc.abstractmethod
    def readData(self,readDataRequest): pass
    def run(self): pass
