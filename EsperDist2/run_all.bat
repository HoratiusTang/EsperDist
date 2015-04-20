@echo off
start run_coordinator.bat -this.id Coordinator -this.port 7123 -qf ./query/queries.txt

ping 0.0.0.0 -n 2 > nul
start run_worker.bat -this.id Worker1 -this.port 9001 -lqr false
start run_worker.bat -this.id Worker2 -this.port 9002 -lqr false
:start run_worker.bat -this.id Worker3 -this.port 9003 -lqr false
start run_worker.bat -this.id Worker4 -this.port 9004 -lqr false
:start run_worker.bat -this.id Worker5 -this.port 9005 -lqr false

ping 0.0.0.0 -n 2 > nul
start run_spout.bat -this.id Spout01 -this.port 10001 -event.category A -event.name AJ
:start run_spout.bat -this.id Spout02 -this.port 10002 -event.category A -event.name AK
start run_spout.bat -this.id Spout03 -this.port 10003 -event.category B -event.name BJ
:start run_spout.bat -this.id Spout04 -this.port 10004 -event.category B -event.name BK
start run_spout.bat -this.id Spout05 -this.port 10005 -event.category C -event.name CJ
ping 0.0.0.0 -n 2 > nul
:start run_spout.bat -this.id Spout06 -this.port 10006 -event.category C -event.name CK
start run_spout.bat -this.id Spout07 -this.port 10007 -event.category D -event.name DJ
:start run_spout.bat -this.id Spout08 -this.port 10008 -event.category D -event.name DK
start run_spout.bat -this.id Spout09 -this.port 10009 -event.category E -event.name EJ
:start run_spout.bat -this.id Spout10 -this.port 10010 -event.category E -event.name EK