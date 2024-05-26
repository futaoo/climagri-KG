import random
import requests
import os

from urllib3.exceptions import MaxRetryError, NewConnectionError

""" 
Class to handle data from the NCEI collection API 
"""


class WeatherDataCollection:
    # base url according to NCEI API documentation
    base_url = "https://www.ncei.noaa.gov/access/services/data/v1?dataset={dataset}&dataTypes=AWND,PRCP,SNWD,SNOW," \
               "WSF2,TAVG,TMAX,TMIN&stations={station}&startDate={startDate}&endDate={endDate}" \
               "&includeStationName=true&includeStationLocation=1&units=metric"

    # List of GHCN-D weather stations given by NOAA at
    # https://www.ncei.noaa.gov/products/land-based-station/global-historical-climatology-network-daily
    def __init__( self, stations_file ):
        self.stations_file = stations_file
        self._create_stations_list()
        self._group_stations()
        self._reduce_stations()

        # for continuing data collection after interim stoppage
        self.data_file = "data/yearly_weather_data.csv"
        self.index_file = "data/yearly_weather_data_index.txt"
        self.index = 0
        if os.path.exists( self.index_file ):
            with open( self.index_file, "r" ) as f:
                self.index = int( f.read().strip() )

    # NCEI API uses http get requests so the url should be formed correctly
    def _create_url( self, dataset, station, start_date, end_date ):
        # TODO: error handling of inputs
        self.url = self.base_url.format( dataset = dataset, station = station, startDate = start_date,
                                         endDate = end_date )

    def collect_data( self ):
        # iterate over GHCN-D stations file
        for index, stations in enumerate( self.grouped_stations.values() ) :
            if index < self.index :
                continue
            for station_id in stations :
                # gather and store data
                print(station_id)
                self._create_url( "global-summary-of-the-year", station_id, "2004-01-01", "2014-12-31" )
                weather_dataset = self._make_get_request( self.url )

                if weather_dataset is not None :
                    self._write_data_to_csv( weather_dataset.content, self.data_file )
                    self._write_to_index_file( index )

            print( "{} done\n".format( index + 1 ) )

    def _make_get_request( self, url ):
        try:
            weather_dataset = requests.get( url )
        except MaxRetryError as e:
            print( "MaxRetryError occurred: ", e )
            return None
        except NewConnectionError as e:
            print( "NewConnectionError occurred: ", e )
            return None

        return weather_dataset

    def _write_data_to_csv( self, data, csv ):
        with open( csv, "ab" ) as csv_file :
            csv_file.write( data )

    def _parse_station_id( self, row ):
        fields = row.split( ' ' )
        return fields[ 0 ]

    def _write_to_index_file( self, i ):
        self.index = i + 1
        with open( self.index_file, "w" ) as f:
            f.write( str( self.index ) )


    def _create_stations_list( self ):
        self.stations_list = []
        with open( self.stations_file ) as stations_file :
            for line in stations_file :
                self.stations_list.append( self._parse_station_id( line ) )

    # method to collect stations in same country
    def _group_stations( self ):
        self.grouped_stations = {}
        for station in self.stations_list :
            code = station[ : 3 ]
            if code in self.grouped_stations :
                self.grouped_stations[ code ].append( station )
            else :
                self.grouped_stations[ code ] = [ station ]

    # reduce number of stations per country to reduce dataset size
    def _reduce_stations( self ):
        for code in self.grouped_stations :
            length = len( self.grouped_stations[ code ] )

            number_to_be_removed = 0
            if length < 1500 :
                number_to_be_removed = (length / 3) * 2
            else :
                number_to_be_removed = (length / 4) * 3
            for i in range( int( number_to_be_removed ) ) :
                del self.grouped_stations[ code ] [
                    random.randrange( len( self.grouped_stations[ code ] ) ) ]

