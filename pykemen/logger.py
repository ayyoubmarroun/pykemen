from datetime import datetime
import sys
verbose = "--verbose" in sys.argv or "-v" in sys.argv
if verbose:
    def verboseprint(*args):
        # Print each argument separately so caller doesn't need to
        # stuff everything to be printed into a single string
        for arg in args:
           print (arg,)
        print ()
else:
    verboseprint = lambda *a: None      # do-nothing function