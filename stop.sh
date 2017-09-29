#!/bin/bash
ps aux | grep java |grep river | awk '{print $2}' | xargs kill -9
ps aux | grep java |grep river | awk '{print $2}'

