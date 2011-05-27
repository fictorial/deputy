compile:
	coffee -c -o bin src/deputy-server.coffee
	echo '#!/usr/bin/env node' >tmp
	cat bin/deputy-server.js >>tmp
	mv tmp bin/deputy-server.js
	chmod +x bin/deputy-server.js

npm: compile
	npm publish .

clean:
	rm -rf bin

.PHONY: compile npm clean
