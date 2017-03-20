#!/bin/bash
gnome-terminal --command="python distributed_queue.py 127.0.0.1:10001 127.0.0.1:10002 127.0.0.1:10003 127.0.0.1:10004"
gnome-terminal --command="python distributed_queue.py 127.0.0.1:10002 127.0.0.1:10001 127.0.0.1:10003 127.0.0.1:10004"
gnome-terminal --command="python distributed_queue.py 127.0.0.1:10003 127.0.0.1:10002 127.0.0.1:10001 127.0.0.1:10004"
gnome-terminal --command="python distributed_queue.py 127.0.0.1:10004 127.0.0.1:10002 127.0.0.1:10003 127.0.0.1:10001"