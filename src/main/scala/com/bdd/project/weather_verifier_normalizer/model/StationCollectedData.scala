package com.bdd.project.weather_verifier_normalizer.model


case class StationCollectedData ( stationId:Integer,
                                  data : List[CollectedChannelsSample]) extends Serializable {
}
