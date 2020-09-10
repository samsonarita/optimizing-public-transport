"""Defines trends calculations for stations"""
import logging
from dataclasses import asdict, dataclass
import json

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
stationsevents = app.topic("stations.events", value_type=Station)
# TODO: Define the output Kafka Topic
transformedevents = app.topic("stations.transformed.events", partitions=1)
# TODO: Define a Faust Table
table = app.Table(
    "stations-table",
    default=int,
    partitions=1,
    changelog_topic=transformedevents,
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(stationsevents)
async def stationsevents(stationsevents):
    async for stationsevent in stationsevents:
        transformed = TransformedStation(
            station_id=stationsevent.station_id,
            station_name=stationsevent.station_name,
            order=stationsevent.order,
            line={stationsevent.red:'red',
                  stationsevent.blue:'blue',
                  stationsevent.green:'green'}[True]
        )

        await transformedevents.send(key=transformed.station_id,
                                     value=transformed)        

if __name__ == "__main__":
    app.main()
