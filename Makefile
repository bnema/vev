.PHONY: install test test-installer lint mocks demo remote-acceptance

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

remote-acceptance:
	scripts/remote-picker-harness/run.sh

demo:
	docker build -f scripts/demo/Dockerfile -t vev-demo .
	./scripts/demo/run.sh demo.tape
	# vhs cannot emit a transparent margin, so cut the window out after the
	# fact: everything outside the rounded window rect becomes GIF
	# transparency, plus a 2px edge stroked along the window. The geometry
	# mirrors demo.tape (1200x750 canvas, margin 20, border radius 10); keep
	# them in sync. ffmpeg streams the clip frame by frame, so peak memory
	# stays flat; the previous imagemagick -coalesce pipeline needed the whole
	# ~22GiB uncompressed clip at once and either crawled for an hour on its
	# disk cache or got OOM-killed — do not go back to it.
	docker run --rm --entrypoint magick -v "$(CURDIR)/scripts/demo/out:/out" vev-demo \
		-size 1200x750 xc:none -fill none -stroke "#30363D" -strokewidth 2 \
		-draw "roundrectangle 21,21 1178,728 9,9" /out/border.png
	docker run --rm --entrypoint magick -v "$(CURDIR)/scripts/demo/out:/out" vev-demo \
		-size 1200x750 xc:black -fill white \
		-draw "roundrectangle 20,20 1179,729 10,10" /out/mask.png
	# alphamerge applies the mask after the border overlay so the mask clips
	# the stroke's antialiased overflow, which a GIF's 1-bit alpha cannot
	# represent; paletteuse's alpha_threshold folds the rest into that 1-bit
	# alpha (palettegen reserves the transparent slot).
	docker run --rm --entrypoint ffmpeg -v "$(CURDIR)/scripts/demo/out:/out" vev-demo \
		-y -v error -i /out/demo.gif -i /out/border.png -i /out/mask.png \
		-filter_complex "[0:v][1:v]overlay=0:0[bordered];[2:v]format=gray[m];[bordered][m]alphamerge,split[a][b];[a]palettegen=reserve_transparent=1[p];[b][p]paletteuse=alpha_threshold=128" \
		/out/demo-masked.gif
	# Keep the final GIF lossless: the streaming FFmpeg pass has already
	# bounded memory, and preserving terminal chrome avoids color banding.
	docker run --rm --entrypoint gifsicle -v "$(CURDIR)/scripts/demo/out:/out" vev-demo --no-conserve-memory -O3 -o /out/demo.gif /out/demo-masked.gif
	# Temp files are owned by the container's subuid, so remove them from
	# inside a container rather than the host.
	docker run --rm --entrypoint sh -v "$(CURDIR)/scripts/demo/out:/out" vev-demo -c 'rm /out/demo-masked.gif /out/mask.png /out/border.png'
	mkdir -p docs/assets
	cp scripts/demo/out/demo.gif docs/assets/demo.gif
