MONGODB_COURSE_DB_NAME="PLMP"
from urllib.parse import quote_plus
username = quote_plus("plmp_admin")
password = quote_plus("admin@1234")

# Create the connection URI
# MONGODB_HOST_1 = f"mongodb://{username}:{password}@localhost:27017"
# MONGODB_HOST_1 =  "mongodb://plmp_admin:admin%401234@52.201.29.59:27017"
MONGODB_HOST_1 =  "mongodb+srv://selva:selva777@cluster0.ekbeh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
front_end_ip = "http://192.168.31.139:3000"
from datetime import timedelta
SIMPLE_JWT = { 
  'ACCESS_TOKEN_LIFETIME': timedelta(minutes=500),
  'ALGORITHM': 'HS256',
  'SIGNING_KEY': 'u22h&79gj6o_q^sd*t(h6lbqc8w!zk!1vbk3b_st$s^97tsn3i',
  'SESSION_COOKIE_DOMAIN' : front_end_ip,
  'SESSION_COOKIE_MAX_AGE' : 120000000000,
  'AUTH_COOKIE': 'access_token', 
  'AUTH_COOKIE_SECURE': False,    
  'AUTH_COOKIE_SAMESITE': 'None', 
}
