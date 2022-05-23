.PHONY: clean data lint requirements sync_data_to_s3 sync_data_from_s3 test
.DEFAULT_GOAL := help

#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
BUCKET = map-input-output/g
PROFILE = default
PROJECT_NAME = g
PYTHON_INTERPRETER = python3

ifeq (,$(shell which conda))
HAS_CONDA=False
else
HAS_CONDA=True
endif

#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Set up python interpreter environment and install/update dependencies
create_environment: venv/bin/activate requirements

## Install or update packages based on requirements.txt
requirements: venv/bin/activate requirements.txt
	. venv/bin/activate; \
	$(PYTHON_INTERPRETER) -m pip install -r requirements.txt; \
    $(PYTHON_INTERPRETER) -m ipykernel install --user --name=$(PROJECT_NAME)
	@echo "Requirements up to date & kernal available in notebook as $(PROJECT_NAME) - restart kernel for changes to take effect"
    @echo ">>> virtualenv created/updated. Activate with: \n(mac) source venv/bin/activate \n(Windows) venv\Scripts\activate"
	touch requirements.txt

#### Install Python Dependencies (not in virtualenv)
venv/bin/activate:
	# Create venv folder if doesn't exist. Run make clean to start over.
	test -d venv || $(PYTHON_INTERPRETER) -m venv venv
	. venv/bin/activate; \
	$(PYTHON_INTERPRETER) -m pip install -U pip setuptools wheel ipykernel
	@echo ">>> virtualenv created/updated. Activate with: \n(mac) source venv/bin/activate \n(Windows) venv\Scripts\activate"
	touch venv/bin/activate

## Make Dataset
data: requirements
	$(PYTHON_INTERPRETER) src/data/make_dataset.py

## Delete all compiled Python files and virtualenv
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete
	#find . -type d -name "*.egg-info" -exec rm -rf {} \;
	rm -rf dist/
	rm -rf venv/
	rm -rf wheelhouse/

## Lint using flake8
lint:
	flake8 src

## Run tests
test: create_environment
	. venv/bin/activate; \
	$(PYTHON_INTERPRETER) -m unittest discover test

## Package dependencies to requirements.txt
package:
	pip freeze > requirements.txt

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
