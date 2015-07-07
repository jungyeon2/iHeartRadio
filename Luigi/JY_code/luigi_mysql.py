__author__ = 'jungyeonyoon'

import ConfigParser
import oauth2 as oauth
import urllib2 as urllib
import json
import MySQLdb
import luigi
# import PyMySQL


def read_config(section_name, filename):

    config = ConfigParser.ConfigParser()
    config.read(filename)

    return config.items(section_name)




# See assignment1.html instructions or README for how to get these credentials

api_key = "RmtiJc07cMrSYoFUwbokQtcY9"
api_secret = "ZkdQ25btgmZATQFwuQKy5OHicTHSexaUC3jPfEvOnmu6pNdjnf"
access_token_key = "176684082-DnVnvjYT9yfi6C2hR9A7FMmNnvrrmaBHjZJ2GzWM"
access_token_secret = "aROC74IYAtC2cvmZlQy46CXsyYjUzgavNWe43Pv0Wbkd4"

_debug = 0

oauth_token    = oauth.Token(key=access_token_key, secret=access_token_secret)
oauth_consumer = oauth.Consumer(key=api_key, secret=api_secret)

signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

http_method = "GET"


http_handler  = urllib.HTTPHandler(debuglevel=_debug)
https_handler = urllib.HTTPSHandler(debuglevel=_debug)

'''
Construct, sign, and open a twitter request
using the hard-coded credentials above.
'''
def twitterreq(url, method, parameters):
    req = oauth.Request.from_consumer_and_token(oauth_consumer,
                                             token=oauth_token,
                                             http_method=http_method,
                                             http_url=url,
                                             parameters=parameters)

    req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)

    headers = req.to_header()

    if http_method == "POST":
        encoded_post_data = req.to_postdata()

    else:
        encoded_post_data = None
        url = req.to_url()

    opener = urllib.OpenerDirector()
    opener.add_handler(http_handler)
    opener.add_handler(https_handler)

    response = opener.open(url, encoded_post_data)

    return response


def mysql_connect():
    db = MySQLdb.connect(host="localhost", # your host, usually localhost
                     # user="", # your username
                     #  passwd="megajonhy", # your password
                      db="test") # name of the data base

    db = MySQLdb.connect(host='localhost', db='test')
    return db

# def sql_insert_row(cur, table_name, list_of_col, vals):
# def sql_insert_row(table_name, json_line):
#     with open('tbl_col_val_pair.txt', 'r') as f:
#         for line in f.readlines():
#             print json.loads(line)


def make_sql_str(val):

    if type(val) is int:
        return str(val) +', '
    else:
        if type(val) is unicode:
            return '"'+val.encode('ascii','ignore') + '", '
        else:
            return '"'+str(val).encode("utf-8") + '", '

def sql_insert_row(db, cursor, table_name, json_file):
    with open('tbl_col_val_pair.txt','r') as f:
        for line in f.readlines():
            j = json.loads(line)
            if table_name in j.keys():
                # insert table into

                qry = "INSERT INTO " + table_name + "("

                qry += (" ,".join(str(bit) for bit in j[table_name].values())).strip('')+") VALUES ("

                for k in j[table_name].keys():
                    l = str(k.replace("$"," ")).split()
                    if len(l) == 1:
                        qry += make_sql_str(json_file[l[0]])
                        # qry += str(json_file[l[0]]).encode("utf-8") + ", "
                    else:
                        a = json_file[l[0]]
                        for key in l[1:]:
                            a = a[key]

                        # print a
                        qry += make_sql_str(a)

                        # if type(a) is unicode:
                        #     qry += a.encode('ascii','ignore') + ", "
                        #     qry += gettext.gettext(str(a.encode('ascii', 'ignore'))) + ", "
                        # else:
                        #     qry += str(a).encode("utf-8") + ", "
                        # else:
                        #     qry += gettext.gettext(str(a).encode('unicode', 'ignore')) + ", "
                qry = qry[:-2] + ")"
                print qry
                cursor.execute(qry)
                db.commit()


                        # print json_file
                        # print json_file[l[0]][l[1]]

                    # json_file['lang']
                    # json_file['user']['gasdgds'][][][]
                    # print json_file[a]
                    # print j[a]

                # qry += "VALUES(" (" ,'".join(json_file[str(bit)]+"'" for bit in j['user'].values())).strip('')+") "


                # for i in range(0, len(j['user'].values())-1):
                #     qry += j['user'].values()[i] +", "
                #
                # print qry + j['user'].values()[len(j['user'].values())-1] + ") VALUES ("




def fetchsamples():


    with open('tweeter_urls.txt', 'r') as f:
        db = mysql_connect()
        cursor = db.cursor()

        for url in f:
            parameters = []
            response = twitterreq(url, "GET", parameters)
            for line in response:

                j = json.loads(line)
                for i in j['statuses']:
                    # print i['user']['name']
                    sql_insert_row(db, cursor,'user', i)
        cursor.close()
        db.close()


  # parameters = []
  # response = twitterreq(url, "GET", parameters)
  # for line in response:
  #   print line.strip()

if __name__ == '__main__':
    fetchsamples()
