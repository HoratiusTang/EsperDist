@echo off
start run_coordinator.bat -this.id Coordinator -this.port 7123 -qf ./query/queries2_filter-join_0500_0.25_0.50.txt -kryonet.wbs 5120000 -kryonet.obs 2048000 -spout.count 8 -epl.interval 1000

ping 0.0.0.0 -n 2 > nul
start run_worker.bat -this.id Worker01 -this.port 9001 -lqr false -kryonet.wbs 5120000 -kryonet.obs 10240000 -worker.nprct 2 -worker.npubt 1
start run_worker.bat -this.id Worker02 -this.port 9002 -lqr false -kryonet.wbs 5120000 -kryonet.obs 10240000 -worker.nprct 2 -worker.npubt 1
start run_worker.bat -this.id Worker03 -this.port 9003 -lqr false -kryonet.wbs 5120000 -kryonet.obs 10240000 -worker.nprct 2 -worker.npubt 1

ping 0.0.0.0 -n 2 > nul
start run_spout.bat -this.id Spout01 -this.port 10001 -event.category A -event.name A -kryonet.wbs 40960000 -kryonet.obs 2048000 -spout.bc 10 &
start run_spout.bat -this.id Spout02 -this.port 10003 -event.category B -event.name B -kryonet.wbs 40960000 -kryonet.obs 2048000 -spout.bc 10 &
start run_spout.bat -this.id Spout03 -this.port 10004 -event.category C -event.name C -kryonet.wbs 40960000 -kryonet.obs 2048000 -spout.bc 10 &
start run_spout.bat -this.id Spout04 -this.port 10006 -event.category D -event.name D -kryonet.wbs 40960000 -kryonet.obs 2048000 -spout.bc 10 &
start run_spout.bat -this.id Spout05 -this.port 10005 -event.category E -event.name E -kryonet.wbs 40960000 -kryonet.obs 2048000 -spout.bc 10 &
start run_spout.bat -this.id Spout06 -this.port 10007 -event.category F -event.name F -kryonet.wbs 40960000 -kryonet.obs 2048000 -spout.bc 10 &
start run_spout.bat -this.id Spout07 -this.port 10009 -event.category G -event.name G -kryonet.wbs 40960000 -kryonet.obs 2048000 -spout.bc 10 &
start run_spout.bat -this.id Spout08 -this.port 10010 -event.category H -event.name H -kryonet.wbs 40960000 -kryonet.obs 2048000 -spout.bc 10 &