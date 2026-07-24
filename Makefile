.PHONY: install test test-installer lint mocks demo

install:
	go install .
	@bin="$$(go env GOBIN)"; \
	if [ -z "$$bin" ]; then bin="$$(go env GOPATH)/bin"; fi; \
	mkdir -p "$$bin"; \
	install -m 0755 scripts/vev-bar-top-right scripts/vev-bar-bottom-right "$$bin"

test:
	go test ./... -race

test-installer:
	sh scripts/install_platform_test.sh

lint:
	@test -z "$$(goimports -l .)"
	go vet ./...

mocks:
	@version="$$(mockery version 2>/dev/null || true)"; \
	if [ "$$version" != "v3.7.1" ]; then \
		echo "mockery v3.7.1 required; install with: go install github.com/vektra/mockery/v3@v3.7.1" >&2; \
		exit 1; \
	fi
	mockery

demo:
	docker build -f scripts/demo/Dockerfile -t vev-demo .
	./scripts/demo/run.sh demo.tape
	# vhs cannot emit a transparent margin, so cut the window out after the
	# fact: everything outside the rounded window rect becomes GIF
	# transparency. The geometry mirrors demo.tape (1200x750 canvas, margin
	# 20, border radius 10); keep them in sync.
	docker run --rm --entrypoint magick -v "$(CURDIR)/scripts/demo/out:/out" vev-demo \
		-size 1200x750 xc:black -fill white \
		-draw "roundrectangle 20,20 1179,729 10,10" /out/mask.png
	docker run --rm --entrypoint magick -v "$(CURDIR)/scripts/demo/out:/out" vev-demo \
		-limit memory 8GiB -limit map 12GiB /out/demo.gif -coalesce \
		null: \( /out/mask.png -alpha off \) -compose CopyOpacity \
		-layers composite /out/demo-masked.gif
	# --lossy above 30 smears the near-identical darks of the window chrome
	# (window bar vs terminal background) into visible banding, and the
	# conserve-memory fallback for huge inputs roughly doubles the output.
	docker run --rm --entrypoint gifsicle -v "$(CURDIR)/scripts/demo/out:/out" vev-demo --no-conserve-memory -O3 --lossy=30 -o /out/demo.gif /out/demo-masked.gif
	rm scripts/demo/out/demo-masked.gif scripts/demo/out/mask.png
	mkdir -p docs/assets
	cp scripts/demo/out/demo.gif docs/assets/demo.gif
