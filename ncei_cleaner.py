import csv


# Function to check if a line contains the desired headers
def is_header( line ):
    headers = [ 'STATION', 'NAME', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'DATE', 'AWND', 'PRCP', 'SNOW', 'SNWD', 'TAVG',
                'TMAX', 'TMIN', 'WSF2' ]
    line = line.strip().split( ',' )
    return line == headers


# Function to check if a line contains the unwanted HTML content
def has_unwanted_html( line ):
    return '<!DOCTYPE HTML PUBLIC "-//IETF//DTD HTML 2.0//EN">' in line or '<html><head>' in line or '<title>503 ' \
                                                                                                     'Service ' \
                                                                                                     'Unavailable' \
                                                                                                     '</title>' in \
           line or '</head><body>' in line or '<h1>Service Unavailable</h1>' in line or '<p>The server is temporarily ' \
                                                                                        'unable to service your' in \
           line or 'request due to maintenance downtime or capacity' in line or 'problems. Please try again ' \
                                                                                'later.</p>' in line or \
           '<p>Additionally' in line or 'error was encountered while trying to use an ErrorDocument to handle the ' \
                                        'request.</p>' in line or '</body></html>' in line


# Path of the input CSV file
input_file = 'data/yearly_weather_data.csv'

# Path of the output CSV file
output_file = 'data/cleaned_yearly_weather_data.csv'


def clean_data():
    # Read the input CSV file and write desired lines to the output file
    with open( input_file, 'r', newline = '' ) as infile, open( output_file, 'w', newline = '' ) as outfile:
        reader = csv.reader( infile )
        writer = csv.writer( outfile )

        header_written = False
        for line in reader:
            # Check if the line is the header line

            if is_header( ','.join( line ) ) :
                if not header_written :
                    writer.writerow( line )
                    header_written = True
            # Check if the line contains unwanted HTML content

            elif not has_unwanted_html( ','.join( line ) ) :
                writer.writerow( line )

    print( "Cleaning complete!" )
