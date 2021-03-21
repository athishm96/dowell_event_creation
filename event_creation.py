import MySQLdb

db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                     user="user",         # your username
                     passwd="pwd",  # your password
                     db="db")        # name of the data base


cursor = db.cursor()

def is_not_blank(s):
    return bool(s and not s.isspace())

def close_connection():
    cursor.close()
    connection.close()

def get_id(table, code):
    query = "SELECT id FROM" + table + "WHERE code='" + code + "';"
    cursor.execute(query)
    results = cursor.fetchall()
    return results[0][0]

def create_event(platform,city,day,database,process,object_id,instance_id,context,document_id):
    ip_address = track_ip()
    login_id=login()
    session=session()
    regional_time=regional_clock()
    dowell_time=dowell_clock()
    location=location()
    if platform != get_id('platform_table_name','01'):
        return 'platform code is not matching'
    if city != get_id('city_table_name','101'):
        return 'city code is not matching'    
    if day != get_id('day_table_name','0001'):
        return 'day code is not matching'  
    if database != get_id('database_table_name','01'):
        return 'city code is not matching'  
    if process != get_id('process_table_name','000001'):
        return 'process code is not matching'  
    if object_id != get_id('object_table_name','101'):
        return 'object code is not matching'  
    if not is_not_blank(instance_id):
        return 'Instance Id is null'
    if isinstance(context, list) and not context:
        return 'context is empty'
    if len(document_id)<24:
        document_id=document_id.zfill(24-len(document_id))
    if len(document_id)>24:
        return 'Invalid document id'
    event_id=platform+city+day+document_id
    if len(event_id) !=33:
        return 'Error in Event Id Creatio
    sql = "INSERT INTO events (eventId, DatabaseId,IP,login,session,process,regional_time,dowell_time,location,object,instane_id,context) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    val =( event_id,database,ip_address,login_id,session,process,regional_time,dowell_time,location,object_id,instance_id,context)
    error = {
    "number": 0,
    "description": 0
    }
    try:
        cursor.execute(sql, val)
        id=cursor.lastrowid
        output={
        "id": id,
        "eventId":event_id,
        "error":error
        }
        close_connection()
        return output
    except Exception as ex:
        error['number']=500
        error['description']=ex.message
        close_connection()
        return error


