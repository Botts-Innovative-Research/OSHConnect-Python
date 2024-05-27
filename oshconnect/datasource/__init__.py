def isDefined(v):
    return v is not None

def randomUUID():
    return str(v)

class Mode(enum):
    REPLAY = "replay"
    BATCH = "batch"
    REAL_TIME = "realTime"

DATASOURCE_DATA_TOPIC = 'datasource-data-'
batchsize = 1


class DataSourceWorker:
    def __init__(self,dataSourceHandlers):
        self.dataSourceHandlers = dataSourceHandlers

class DataSourceHandler:
    def __init__(self,context,topic,broadcastChannel,values,version,properties,initialized):
        self.context = any
        self.topic = any
        self.broadcastChannel = any
        self.values = []
        self.values = 0
        self.properties = batchsize
        self.initialized = False


def handleIsInit(self,eventData, resp):
    dsId = eventData.dsId
    resp.data = self.dataSourceHandlers(dsId).isInitalized()
    self.postMessage(resp)


        
