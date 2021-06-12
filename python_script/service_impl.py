from dataIngestionImpl import *
from flowControl import FlowControl
import json
import pyspark.sql.functions as F
from pyspark.sql import Row
from datetime import datetime
import traceback
import csv
import sys
import io
from lxml import etree
from io import BytesIO
import re
from pyspark.sql.types import *

if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO


class Example(FlowControl):
    def __init__(self):
        super().__init__()
        self.today = datetime.now()

    def readDataImplCustom(self, readDataRequestObj):
        self.logger.info("Example - readDataImplCustom [{}]".format(self.app_name))
        return_response = ReadDataResponse()
        return return_response

    def buildDataFrameCustom(self, buildDataFrameRequest):
        self.logger.info("Example - buildDataFrameCustom [{}]".format(self.app_name))
        return BuildDataFrameResponse()

    def buildDataFrame(self, buildDataFrameRequest):
        self.logger.info("Example - buildDataFrame [{}]".format(self.app_name))
        buildDataFrameResponse = BuildDataFrameResponse()
        try:
            data = buildDataFrameRequest.data()
            if data is None:
                return None
            return buildDataFrameResponse
        except Exception as e:
            traceback.print_exc()
            self.manageException(e, ErrorException.APPLICATION_GENERIC_ERROR, buildDataFrameResponse)
        finally:
            return buildDataFrameResponse


##### ESECUZIONE #####
b = Example()
b.run()