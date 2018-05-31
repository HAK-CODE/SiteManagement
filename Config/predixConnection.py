import predix
import predix.data.timeseries


def connectionObject():
    app = predix.app.Manifest('./Config/manifest.yml')
    timeSeries = app.get_timeseries()
    return timeSeries


timeSeries = connectionObject()
