import sys
from tqdm import tqdm
import foastat_node_creation
import ncei_data_collection
import faostat_data_collection
import data_frame_IO
import ncei_node_creation
import ncei_cleaner
import country_node_creation
import Neo4j

""" 
Main co-ordination of data collection, processing and graph implementation 
"""

graph = Neo4j.Graph( "bolt://localhost:7687", "neo4j", "FYP2023" )


def main( arg ):
    years = [ 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014 ]

    # Data Collection
    if arg == "weather":
        weather_collection = ncei_data_collection.WeatherDataCollection( "resources/stations/ghcnd-stations.txt" )
        weather_collection.collect_data()

    elif arg == "crop_yield":
        all_elements = faostat_data_collection.write_pars_to_file( "elements", "QCL", "qcl_elements.txt" )
        all_items = faostat_data_collection.write_pars_to_file( "items", "QCL", "qcl_items.txt" )
        all_items = all_items[ :-5 ]  # Extract the first two elements using slicing

        crops_livestock_collection = faostat_data_collection.FaostatDataCollection( "QCL", [ 2413 ], all_items, years,
                                                                                    "QCL_data.csv" )
        crops_livestock_collection.collect_data()

    elif arg == "nutrition":
        all_elements = faostat_data_collection.write_pars_to_file( "elements", "FBS", "fbs_elements.txt" )
        all_items = faostat_data_collection.write_pars_to_file( "items", "FBS", "fbs_items.txt" )
        all_items = all_items[ : -5 ]

        nutrition_collection = faostat_data_collection.FaostatDataCollection( "FBS", [ 661, 671, 681 ], all_items,
                                                                              years, "FBS_data.csv" )
        nutrition_collection.collect_data()

    elif arg == "emissions":
        all_elements = faostat_data_collection.write_pars_to_file( "elements", "GT", "gt_elements.txt" )
        all_items = faostat_data_collection.write_pars_to_file( "items", "GT", "gt_items.txt" )
        all_items = all_items[ : -6 ]

        emissions_collection = faostat_data_collection.FaostatDataCollection( "GT", [ 7273, 724413, 717815 ], all_items,
                                                                              years, "GT_data.csv" )
        emissions_collection.collect_data()

    # Data Cleaning
    elif arg == "clean":
        ncei_cleaner.clean_data()

    # View current data layout
    elif arg == 'layout':
        df_list = data_frame_IO.create_df_from_csv( [ 'data/FBS_data.csv',
                                                      'data/GT_data.csv',
                                                      'data/QCL_data.csv',
                                                      'data/yearly_weather_data.csv' ] )
        data_frame_IO.write_dtypes_to_file( df_list, [ "FBS", "GT", "QCL", "Weather" ], "resources/data_layouts"
                                                                                        "/data_layouts.txt" )

    # Graph Creation
    elif arg == 'graph':
        # Node creation
        # country_node_creation.create_country_nodes()
        # ncei_node_creation.create_nodes()
        # foastat_node_creation.create_gt_nodes()
        foastat_node_creation.create_nodes( "data/FBS_data.csv", range( 2010, 2014 ), "resources/items_and_elements"
                                                                                      "/fbs_items.txt",
                                            "resources/fbs_nodes.txt",
                                            [ 'Food supply (kcal)', 'Fat supply quantity (g)',
                                              'Protein supply quantity (g)' ], "Nutrition" )
        foastat_node_creation.create_nodes( "data/QCL_data.csv", range( 2004, 2014 ), "resources/items_and_elements"
                                                                                      "/qcl_items.txt",
                                            "resources/qcl_nodes.txt", [ 'Yield' ], "Crop" )

        # Create relationships
        progress_bar = tqdm( total = 5, desc = "Creating Relationships", unit = "relationships" )
        graph.create_relationship_by_properties( "Crop", "Weather", [ "Area", "Year" ], [ "Country_Name", "DATE" ],
                                                 "HAS_WEATHER" )
        progress_bar.update( 1 )
        graph.create_relationship_by_properties( "Country", "Crop", [ "Name" ], [ "Area" ], "PRODUCES" )
        progress_bar.update( 1 )
        graph.create_relationship_by_properties( "Country", "Greenhouse_gases", [ "Name" ], [ "Area" ], "PRODUCES" )
        progress_bar.update( 1 )
        graph.create_relationship_by_properties( "Country", "Nutrition", [ "Name" ], [ "Area" ], "HAS_NUTRITION" )
        progress_bar.update( 1 )
        graph.create_relationship_by_properties( "Country", "Weather", [ "Code" ], [ "STATION" ], "HAS_WEATHER" )
        progress_bar.update( 1 )

        progress_bar.close()

    else:
        print( "Invalid argument provided" )


if __name__ == "__main__":
    args = sys.argv
    if len( args ) < 2:
        print( "No argument provided." )
    else:
        main( args[ 1 ] )
