import pandas as pd


def create_df_from_csv( csv ):
    df_list = []
    for csv in csv :
        df_list.append(pd.read_csv( csv, encoding = 'utf-8', encoding_errors = 'ignore' ))
    return df_list


def write_dtypes_to_file( df_list, name_list, file ):
    for df, name in zip( df_list, name_list ) :
        with open( file, "a" ) as f:
            f.write( name + ":\n" )
            f.write( str( df.dtypes ) )
            f.write( '\n\n\n' )
