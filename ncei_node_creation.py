import pandas as pd
import Neo4j
from tqdm import tqdm

graph = Neo4j.Graph( "bolt://localhost:7687", "neo4j", "FYP2023" )


def create_nodes():
    ncei_df = pd.read_csv("data/cleaned_yearly_weather_data.csv")
    country_codes_df = pd.read_csv("resources/Country_codes/Country_codes.txt", sep="\t")

    # Making a single line per country using means
    grouped_by_country = ncei_df.groupby([ncei_df['STATION'].str[:2], 'DATE'])
    means = grouped_by_country.mean(numeric_only=True)
    means = means.reset_index()

    # Merge with country codes dataframe to get country names
    individual_countries = pd.merge(
        means,
        country_codes_df[['Country name', 'FIPS 10-4']],
        how='left',
        left_on='STATION',
        right_on='FIPS 10-4'
    )

    individual_countries.rename(columns={'Country name': 'Country_Name'}, inplace=True)

    # Drop unnecessary columns
    individual_countries = individual_countries.drop('FIPS 10-4', axis=1)

    total_iterations = len(individual_countries)

    # Create a progress bar with the total number of iterations
    progress_bar = tqdm(total=total_iterations, desc="Weather Node Progress", unit="iteration")

    for i, row in individual_countries.iterrows():
        name = row['STATION'] + "_"  + "_WEATHER"
        graph.create_and_return_nodes(row, "Weather", name)
        # Update the progress bar for the current iteration
        progress_bar.update(1)

    # Close the progress bar
    progress_bar.close()

    return individual_countries

