@echo off
start run_coordinator.bat -this.id Coordinator -this.port 7123 -qf ./query/queries.txt

ping 0.0.0.0 -n 3 > nul
start run_worker.bat -this.id Worker1 -this.port 9001
start run_worker.bat -this.id Worker2 -this.port 9002
start run_worker.bat -this.id Worker3 -this.port 9003
start run_worker.bat -this.id Worker4 -this.port 9004
start run_worker.bat -this.id Worker5 -this.port 9005