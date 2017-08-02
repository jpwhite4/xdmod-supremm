#!/bin/bash

versions=$(git branch -r | awk 'BEGIN{FS="/"} /^  origin\/xdmod/{print $2}')

for version in $versions;
do
    filelist=$(git ls-tree --name-only -r origin/$version docs | egrep '*.md$')
    for file in $filelist;
    do
        outfile=$(echo $file | awk 'BEGIN{FS="/"} { for(i=2; i < NF; i++) { printf "%s/", $i } print "'$version'/" $NF}')
        mkdir -p $(dirname $outfile)
        git show refs/remotes/origin/$version:$file > $outfile
    done
done
