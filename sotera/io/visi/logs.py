from . import analytics_map

def format_data_log(header, packet_data):
    adef_ = analytics_map[ header[3] ]
    return adef_['format_str'].format( *packet_data )

def _data_consume_log(tm, data, content, key):
    header, log_msg = content
    data[key].append( [tm, log_msg] )

def _data_consume_analytics(tm, data, content, key):
    if content[1] is not None:
        header, packet_data = content
        adef_ = analytics_map[ header[3] ]
        key =  (adef_['array'],header[3])
        row = [ header[4] ]
        row.extend(packet_data)
        data[key].append(row)
