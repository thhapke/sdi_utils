
import random
import sys
import time
from datetime import date
import json


MAX = 200

def main():

    for i in range(0,MAX) :
        dict_out= {"website":"test.com", "url":"http://xx.com", "date":str(date.today()), "title":"TITLE Something", "index":random.randint(0,MAX), "text":"There are a number of ways you can take to get the current date. We will use the date class of the datetime module to accomplish this task"}
        print(json.dumps(dict_out))
        if i%3 == 0 :
            sys.stderr.write("ERROR: {}: Mod 3".format(i)+"\n")
            time.sleep(0.1)
        time.sleep(0.1)


if __name__ == "__main__":
    main()
    # gs.gensolution(os.path.realpath(__file__), config, inports, outports)

