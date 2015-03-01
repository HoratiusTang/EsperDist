@echo off
start run_spout.bat -this.id Spout1 -this.port 10001 -event.name A
start run_spout.bat -this.id Spout2 -this.port 10002 -event.name B
start run_spout.bat -this.id Spout2 -this.port 10003 -event.name C