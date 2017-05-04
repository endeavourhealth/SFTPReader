#!/bin/bash

for config in /nfsshare/sftpreader/* ; do
	for extract in $config/* ; do
		for split in $extract/Split/*; do

			has_subdirs="false"

			for split_subdirs in $split/*; do
				if [[ -d $split_subdirs ]]; then
					has_subdirs="true"
				fi
			done

			if [[ $has_subdirs = "true" ]]; then
				echo $split
			fi
		done
	done
done
