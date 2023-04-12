.PHONY: init_data clean_data deploy undeploy hotel_prepare clean

hotel_prepare:
	mkdir -p ~/beldi/bin-workflow/hotel
	mkdir -p ~/beldi/internal/hotel-workflow/main
	cp /stateful_serverless/hotel/*.go ~/beldi/internal/hotel-workflow/main
	cp -r /stateful_serverless/hotel/wrapper/ ~/beldi/internal/hotel-workflow/main

hotel: hotel_prepare
	for target in geo rate search profile recommendation user frontend flight hotel order \
			flight-acquire flight-reserve hotel-acquire hotel-reserve; do \
		cd ~/beldi; \
		env GOOS=linux go build -ldflags="-s -w -X github.com/eniac/Beldi/pkg/beldilib.TYPE=BELDI" \
		-o bin-workflow/hotel/$$target internal/hotel-workflow/main/$$target.go; \
	done

hotel_failure: hotel_prepare
	for target in geo rate search profile recommendation user frontend flight hotel order \
			flight-acquire flight-reserve hotel-acquire hotel-reserve; do \
		cd ~/beldi; \
		env GOOS=linux go build -ldflags="-s -w -X github.com/eniac/Beldi/pkg/beldilib.TYPE=BELDI \
		-X github.com/eniac/Beldi/internal/hotel-workflow/main/wrapper.FAILURE_RATE=0.1" \
		-o bin-workflow/hotel/$$target internal/hotel-workflow/main/$$target.go; \
	done

clean:
	rm -rf ~/beldi/bin-workflow
	rm -rf ~/beldi/internal/hotel-workflow

init_data:
	cd ~/beldi; go run internal/hotel/init/init.go clean beldi
	cd ~/beldi; go run internal/hotel/init/init.go create beldi
	cd ~/beldi; go run internal/hotel/init/init.go populate beldi

clean_data:
	cd ~/beldi; go run internal/hotel/init/init.go clean beldi

deploy:
	cp /stateful_serverless/hotel-wf.yml ~/beldi
	cd ~/beldi; sls deploy -c hotel-wf.yml

undeploy:
	cd ~/beldi; sls remove -c hotel-wf.yml
