

import subprocess
import sys
import io
import select
import os


def main():

    cmd = ["python", "outputgen.py"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,stderr=subprocess.PIPE,universal_newlines=True)

    output = '1'
    errout = '1'
    while proc.poll() is None or  output != '' or  errout != '':
        output = proc.stdout.readline()
        errout = proc.stderr.readline()
        if output != '':
            print(output.strip())
        if errout != '':
            print (errout.strip())




if __name__ == '__main__':
    main()
    # gs.gensolution(os.path.realpath(__file__), config, inports, outports)

