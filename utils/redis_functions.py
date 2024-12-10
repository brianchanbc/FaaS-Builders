import redis

#r = redis.Redis(host='localhost', port=6379, decode_responses=True)
#response = r.execute_command('PING')
#print(f"Redis running: {response}") # True if the server is running

def check_key_exists(r, key):
    if r.exists(key):
        print(f"Key {key} exists.")
        return True
    else:
        print(f"Key {key} does not exist.")
        return False

def get_hash_table(r, key):
    hash_table = r.hgetall(key)
    print(f"Hash Table for {key}: {hash_table}")
    return hash_table

def get_hash_table_field(r, key, field):
    field_value = r.hget(key, field)
    print(f"Field {field} for {key}: {field_value}")
    return field_value

def set_hash_table_field(r, key, field, value):
    r.hset(key, field, value)
    print(f"Field {field} for {key} set to {value}")

def get_data(r, key):
    data = r.get(key)
    print(f"Data for {key}: {data}")
    return data

def print_all_keys(r):
    keys = r.keys('*')
    for key in keys:
        print(key)

def delete_everything(r):
    r.flushall()
    
def delete_key(r, key):
    r.delete(key)
    



