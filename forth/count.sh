#!/bin/bash
echo "residents"
grep resident  $1 | grep -v dynamic | wc -l
echo "commuters"
grep commuter $1 | wc -l
echo "dynamic residents"
grep dynamic $1 | wc -l
echo "visitors"
grep visitor $1 | wc -l
