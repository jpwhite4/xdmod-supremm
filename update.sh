#!/bin/bash

branches=$(curl -s https://api.github.com/repos/ubccr/xdmod-supremm/releases | jq .[].target_commitish | grep -o 'xdmod[0-9]\.[0-9]')

for branch in xdmod7.0 $branches;
do
    version=${branch:5}
    filelist=$(git ls-tree --name-only -r origin/$branch docs | egrep '*.md$')
    for file in $filelist;
    do
        outfile=$(echo $file | awk 'BEGIN{FS="/"} { for(i=2; i < NF; i++) { printf "%s/", $i } print "'$version'/" $NF}')
        mkdir -p $(dirname $outfile)
        git show refs/remotes/origin/$branch:$file | sed '/^redirect_from:$/{N;s/^redirect_from:\n    - ""/redirect_from:\n    - "\/'$version'\/"/}'  > $outfile
    done
done
