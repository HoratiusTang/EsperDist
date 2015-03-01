@echo off
start run_coordinator.bat -this.id Coordinator -this.port 7123

ping 0.0.0.0 -n 3 > nul
start run_worker.bat -this.id Worker1 -this.port 9001
start run_worker.bat -this.id Worker2 -this.port 9002
start run_worker.bat -this.id Worker3 -this.port 9003
start run_worker.bat -this.id Worker4 -this.port 9004
start run_worker.bat -this.id Worker5 -this.port 9005

ping 0.0.0.0 -n 2 > nul
//start run_spout.bat -this.id Spout1 -this.port 10001 -event.name A
//start run_spout.bat -this.id Spout2 -this.port 10002 -event.name B
