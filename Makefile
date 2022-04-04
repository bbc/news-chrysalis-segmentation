.PHONY: shiny clean data lint requirements sync_data_to_s3 sync_data_from_s3 test
.DEFAULT_GOAL := help

#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
BUCKET = map-input-output/news_chrysalis_segmentation
PROFILE = default
PROJECT_NAME = news_chrysalis_segmentation
R_COMMAND = Rscript

#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Set up R interpreter environment
create_environment: renv/created.ignore

renv/created.ignore:
	@echo ">>> Installing remotes at system level if not already intalled."
	$(R_COMMAND) -e "if (!requireNamespace('remotes')) install.packages('remotes')"
	@echo ">>> Installing renv at system level if not already intalled."
	$(R_COMMAND) -e "if (!requireNamespace('renv')) remotes::install_github('rstudio/renv')"
	@echo ">>> Initialise renv if not yet exists"
	if [ ! -f "renv/activate.R" ]; then $(R_COMMAND) -e "renv::init()"; fi
	$(R_COMMAND) -e "renv::restore()"
	$(R_COMMAND) -e "renv::activate()"
	@echo ">>> New renv created and activated"
	touch renv/created.ignore

## Sync Dependencies with Lockfile
update_environment: create_environment renv.lock
	$(R_COMMAND)  -e "renv::activate()" -e "renv::restore(library='renv/library')"
	touch renv.lock

## Make Dataset
data: requirements
	$(R_COMMAND) src/data/make_dataset.R

## Delete all temp files (including environment)
clean:
	find . -type f -name "*.RData" -delete
	find renv/ -type f -not -name "activate.R" -not -name "settings.dcf" -delete
	rm -rf renv/library/
	rm -rf .Rproj.user/
	rm -rf man/

# Compile roxygen docs
docs: create_environment
	$(R_COMMAND) --vanilla -e "renv::activate()" -e "devtools::document()"

## Run tests inside renv
test: create_environment
	$(R_COMMAND) --vanilla -e "renv::activate()" -e "devtools::test()"

## Package dependencies to lockfile
package: create_environment test docs
	$(R_COMMAND) --vanilla -e "renv::activate()" -e "renv::snapshot('packrat')"

## Build and run shiny app
shiny: test
	$(R_COMMAND) -e "renv::activate()" -e "if (!requireNamespace('shiny')) install.packages('shiny')"
	$(R_COMMAND) --vanilla -e "renv::activate()" -e "shiny::runApp('shiny', port=8501)"

# Rebuilds docker container if dockerfile changes
docker_build:
	docker-compose build

## Run docker container, will build it first if required
docker: docker_build
	docker-compose up -d
	@echo ">>> Docker container ready. Rserver localhost:8787, Shiny localhost:3838"
	@echo ">>> To open a terminal in container run:"
	@echo ">>> docker exec -it news_chrysalis_segmentation /bin/bash"

## Stop the running container
docker_stop:
	docker-compose down
	@echo ">>> Docker container stopped."

## Upload Data to S3
sync_data_to_s3:
ifeq (default,$(PROFILE))
	aws s3 sync data/ s3://$(BUCKET)/data/
else
	aws s3 sync data/ s3://$(BUCKET)/data/ --profile $(PROFILE)
endif

## Download Data from S3
sync_data_from_s3:
ifeq (default,$(PROFILE))
	aws s3 sync s3://$(BUCKET)/data/ data/
else
	aws s3 sync s3://$(BUCKET)/data/ data/ --profile $(PROFILE)
endif


#################################################################################
# PROJECT RULES                                                                 #
#################################################################################



#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
