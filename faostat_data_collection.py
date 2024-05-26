import csv
import faostat

"""
    method to write allowable pars for faostat api to a text file 
    file can be used for reference later in data collection
    returns a python list of all codes
"""


def write_pars_to_file( parameter_type, dataset, write_file ):
    all_pars = []
    if parameter_type == "elements":
        pars = faostat.get_elements( dataset )
    elif parameter_type == "items":
        pars = faostat.get_items( dataset )

    # create txt file from parameters list gotten from faostat
    with open( "resources/items_and_elements" + write_file, "w" ) as f:
        for item, value in pars.items():
            all_pars.append( value )
            f.write( item + ":" + value + "\n" )

    return all_pars


class FaostatDataCollection:
    def __init__( self, code, elements, items, years, csv_file ):
        self.all_items = []
        self.all_elements = []
        self.dataset_code = code
        self.pars_dictionary = { "elements": elements, "items": items, "years" : years }
        self.csv_file = csv_file

    def collect_data( self ):
        data = faostat.get_data( self.dataset_code, pars = self.pars_dictionary )

        with open( "data/" + self.csv_file, "w", newline = "" ) as csvfile:
            writer = csv.writer( csvfile )
            for row in data:
                writer.writerow( row )