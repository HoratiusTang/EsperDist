bash run_coordinator.sh -this.id Coordinator -this.port 7123 -qf ./query/queries2_filter-join_0200_0.25_0.50.txt -kryonet.wbs 51200000 -kryonet.obs 20480000 -spout.count 8 -epl.interval 1000
#bash run_worker.sh -this.id Worker01 -this.port 9001 -lqr false
#bash run_spout.sh -this.id Spout01 -this.port 10001 -event.category A -event.name AJ
