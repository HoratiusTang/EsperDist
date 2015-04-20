@echo off
ping 0.0.0.0 -n 3 > nul
start run_worker.bat -this.id Worker1 -this.port 9001 -lqr false
start run_worker.bat -this.id Worker2 -this.port 9002 -lqr false
start run_worker.bat -this.id Worker3 -this.port 9003 -lqr false
start run_worker.bat -this.id Worker4 -this.port 9004 -lqr false
start run_worker.bat -this.id Worker5 -this.port 9005 -lqr false