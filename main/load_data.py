import re

def load_data_to_csv(self):
	file_to_convert = 'taxi_data_test'
	num_records_dropped = 0
	num_valid_records = 0

	# Open file to read from
	with open('./data/' + file_to_convert,'r') as in_fp:
    		# Open file to write to
    		with open('./data/' + file_to_convert + '.csv','w') as out_fp:
	        # Each line is of the format:
        	# ((time_cat, time_num, time_cos, time_sin, day_cat, day_num, day_cos, day_sin, weekend, geohash), number of pickups)
	        for line_str in in_fp:
        	    # Bool indicating if record is valid
	            write_line_out = False
            
        	    # Get rid of parens, quotes, spaces
	            line_str = re.sub('[()\'\s]', '', line_str)
	            line = line_str.split(',')
            
       		    # Need to check if the datetime info is valid:
	            # (i.e. if all the data for is records was present in the original datafile)
        	    #     If it is valid, line should have length 11
	            #     If it is not valid, line should be [None, geohash, number of taxis]
        	    if len(line) == 15:
	            # Need to check the geohash is valid:
        	    # (i.e. if latitude/longitude was present in the original datafile)
                	if line[12] is not None:
	                    csv_line = ",".join(line[0:14]) + '\n'
        	            write_line_out = True
            
	            # Check if the line is valid
        	    if write_line_out:
                	out_fp.write(csv_line)
	                num_valid_records += 1
        	    else:
                	num_records_dropped += 1
