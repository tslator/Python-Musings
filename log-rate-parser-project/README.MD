# Log Rate Analyzer
Given a log file with timestamped events, compute:
- events per minute
- busiest 5-minute window
- any gaps > 2 minutes with no events

# Events Per Minute
The intent of the statement is taken to mean the average events per minute over the entire log file
e.g., total events vs total time

# Busiest 5-minute window
Processing involves creating a moving window that is approximately 5 minutes wide.  Starting with the first
event timestamp, expand the window until it exceeds 5 minutes, then reduce the window until it is less and 
continue.  A Deque is a good data structure in this case as we an append new timestamps to the end, and remove
older timestamps from the begining using popleft().  The number of events in the window will be indicated by
the length of the Deque.
e.g. window start = window[0], window stop = window[-1], num of events = len(window)
As the windows are accumulated we track the max events and capture the window start/stop and return them.

# Any gaps > 2 minutes with no events
The problem statement says "any gaps" which is taken to mean the first gap detected.  ChatGPT thought that
"any" meant "all".  I can see a case for both, but the problem statement is clear.
Processing involves comparing timestamps between adjacent events.  Any time between adjacent events greater
than 2 minutes is detected.
