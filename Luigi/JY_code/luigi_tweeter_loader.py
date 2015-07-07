__author__ = 'jungyeonyoon'

import luigi
import luigi_mysql
import oauth2 as oauth
import urllib2 as urllib
import json
import MySQLdb
import cfg_lib


# class luigi_tweeter_loader(luigi.Task):


class Tweeter_URLs(luigi.Task):


    ## Connect mysql db and return db instance and its cursor
    def connect_mysql(self, param_list):
        db =  MySQLdb.connect(host = param_list['host'], user = param_list['user'], passwd = param_list['passwd'], db = param_list['db'])
        cursor = db.cursor()
        return db, cursor

    ## Close db and cursor
    def close_mysql(self, db, cursor):
        db.close()
        cursor.close()


# api_key = "RmtiJc07cMrSYoFUwbokQtcY9"
# api_secret = "ZkdQ25btgmZATQFwuQKy5OHicTHSexaUC3jPfEvOnmu6pNdjnf"
# access_token_key = "176684082-DnVnvjYT9yfi6C2hR9A7FMmNnvrrmaBHjZJ2GzWM"
# access_token_secret = "aROC74IYAtC2cvmZlQy46CXsyYjUzgavNWe43Pv0Wbkd4"

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
# def twitterreq(url, method, parameters):

    def twitterreq(url, method, tweeter_param_dict):



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



    def output(self):
        return luigi.LocalTarget("/Users/jungyeonyoon/Documents/iHeartRadio/Luigi/JY_code/input/input_1.tsv")


    def run(self):
        mysql_params = cfg_lib.read_config('MySQL_test', 'luigi_mysql_config.cfg')
        db, cursor =self.connect_mysql(mysql_params)
        tweeter_api_params = cfg_lib.read_config('Tweeter_API', 'luigi_mysql_config.cfg')
        twitterreq('url',GET',tweeter_api_params)




        # print cfg_dict
        # return

if __name__ == '__main__':
    luigi.run()