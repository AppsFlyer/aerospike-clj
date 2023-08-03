#!/usr/bin/env bash

echo "cleanup..."
rm -rf codox && mkdir codox
git clone git@github.com:AppsFlyer/aerospike-clj.git codox
cd codox
git symbolic-ref HEAD refs/heads/gh-pages
rm .git/index
git clean -fdx
cd ..
echo "regenerate docs..."

lein with-profile +docs codox

echo "commit changes"
cd codox
git add .
git commit -am "Update project documentation"
echo "push updated docs..."
git push -v -f -u origin gh-pages
cd ..
echo "done! see updated docs at: https://appsflyer.github.io/aerospike-clj/index.html"
