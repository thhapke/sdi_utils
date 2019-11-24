# tprogress
Simple time capturing for python, that logs the time passed since last call with logger level.DEBUG. 

# How-to

## Time Logging
1. Create tprogress class instance, e.g. *timep = tprogress.progress()* . Starts the clock.
2. Call *tprogress.elapsed_time (self,stage='')* that returns a formatted string with time and in debug mode the time. 
The argument *stage* is a string that can be used to state at what stage the program is when called. 

Example
  * DEBUG:tprogress:Elapsed Time:   1m 0.683s (  0: 1:0.7) - Backward Calculation of Rank
  

## Time Monitoring
1. Create tprogress class instance, e.g. *timep = tprogress.progress(max_num=100,step_size=1,freq=5)* . Starts the clock.
2. Call *tprogress.monitor()* within a large *for*-loop that tells about how far program is and predicts
when the loop is finished. The setting is done in the _init_ call: 
  * max_num: Number of loops, e.g. 100
  * step_size: Size of step, e.g. 1
  * freq: How often an info print should be done, e.g. 5, every 5th loop iteration
