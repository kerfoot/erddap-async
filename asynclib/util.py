import os
import sys
import csv

def csv2json(csv_filename):
    
    json_array = []
   
    # Check for 0 file size
    if os.stat(csv_filename).st_size == 0:
        sys.stderr.write('{:s}: Empty file\n'.format(csv_filename))
        return json_array

    try:
        fid = open(csv_filename, 'r')
    except IOError as e:
        sys.stderr.write('{:s}: {:s}\n'.format(csv_filename, e.strerror))
        return json_array
        
    csv_reader = csv.reader(fid)
    cols = csv_reader.next()
    col_range = range(0,len(cols))
    
    for r in csv_reader:
    
        if r[0].startswith('#'):
            continue
            
        stream_meta = {cols[i]:r[i] for i in col_range}
        
        json_array.append(stream_meta)
        
    fid.close()
    
    return json_array
