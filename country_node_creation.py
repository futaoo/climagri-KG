import pandas as pd
import Neo4j
from tqdm import tqdm

graph = Neo4j.Graph( "bolt://localhost:7687", "neo4j", "FYP2023" )


def create_country_nodes():
    # Open the file and get the total number of lines
    with open( "resources/Country_codes/country_codes.txt", 'r' ) as countries_file:
        total_lines = sum( 1 for _ in countries_file )

    # Reopen the file to start from the beginning
    with open( "resources/Country_codes/country_codes.txt", 'r' ) as countries_file:
        # Create a progress bar with the total number of lines
        country_node_progress = tqdm( total = total_lines, desc = "Country Node Progress", unit = "line" )

        # Loop over the lines in the file
        for line in countries_file:
            country, code, other_code = line.strip().split( "\t" )
            df = pd.DataFrame( { "Name": [ country ], "Code": [ code ] } )

            create_single_node( df, "Country", country )

            # Update the progress bar
            country_node_progress.update( 1 )

        # Close the progress bar
        country_node_progress.close()


def create_single_node( df, label, name ):
    for index, row in df.iterrows():
        graph.create_and_return_nodes( row, label, name )
