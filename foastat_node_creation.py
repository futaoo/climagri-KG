import pandas as pd
import Neo4j
from tqdm import tqdm
import time

graph = Neo4j.Graph( "bolt://localhost:7687", "neo4j", "FYP2023" )


def create_nodes(data_file, years, items_file, nodes_file, elements, label):
    data = pd.read_csv(data_file, encoding='utf-8', encoding_errors='ignore')
    create_node_file(years, items_file, nodes_file)

    # Open the text file
    with open(nodes_file, "r") as f:
        lines = f.readlines()
        total_iterations = len(lines)

        progress_bar = tqdm(total=total_iterations, desc=f"{label} node progress", unit="iteration")

        # Iterate over each line
        for line in lines:
            # Get the year, item, and country from the line
            year, item, country = line.strip().split(":")

            # Create a temporary dataframe containing the desired columns for the corresponding row in df
            temp_df = data[(data['Year'] == int(year)) & (data['Item'] == item) & (data['Area'] == country) &
                           (data['Element'].isin(elements))]

            if temp_df.empty:
                with open( "output/Lines not found", "a" ) as file :
                    file.write( f"No matching row found for {year}, {item}, {country}\n" )
                continue

            # Put it all in one line to make it a node
            pivot_df = temp_df.pivot_table(index=['Area', 'Item', 'Year'], columns='Element',
                                           values='Value').reset_index()

            name = pivot_df.loc[0]['Item'] + '_' + str(pivot_df['Year']) + '_' + pivot_df['Area'] + '_' + label

            for index, row in pivot_df.iterrows():
                graph.create_and_return_nodes(row, label, name)

            progress_bar.update(1)

        progress_bar.close()


def create_gt_nodes():
    start_time = time.time()

    df = pd.read_csv('data/GT_data.csv', encoding='utf-8', encoding_errors='ignore')

    # group the data by country and year and sum the values
    grouped_df = df.groupby(['Area', 'Year'])['Value'].sum().reset_index()

    total_iterations = len(grouped_df)
    progress_bar = tqdm(total=total_iterations, desc="Greenhouse Gas Nodes", unit="iteration")

    for index, row in grouped_df.iterrows():
        name = row['Area'] + '_' + "emissions"
        graph.create_and_return_nodes(row, "Greenhouse_gases", name)
        progress_bar.update(1)

    progress_bar.close()

    end_time = time.time()
    execution_time = end_time - start_time


def create_node_file( years, items_file, nodes_file ):
    with open( items_file, 'r' ) as items_file:
        items = items_file.readlines()[ 1: ]
        items = [ line.strip().split( ':' )[ 0 ].strip() for line in items ]

    with open( 'resources/Country_codes/country_codes.txt', 'r' ) as countries_file:
        countries = countries_file.readlines()[ 1: ]
        countries = [ line.strip().split( '\t' )[ 0 ].strip() for line in countries ]

    with open( nodes_file, 'w' ) as output_file:
        for year in years:
            for item in items:
                for country in countries:
                    output_file.write( str( year ) + ':' )
                    output_file.write( item + ':' )
                    output_file.write( country + '\n' )
