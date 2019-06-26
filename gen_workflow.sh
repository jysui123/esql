#!/bin/bash

RANDOM=$$
MAXET=1000
MAXDT=200
for i in {1..10}; do
    ET=$(($RANDOM%$MAXET))
    DT=$(($RANDOM%$MAXDT))
    COSTINT=$(($RANDOM%$MAXET))
    KEYWORD=$()
    ./cadence --do samples-domain workflow run --tl helloWorldGroup --wt main.Workflow \
              --et $ET --dt $DT -i '"jysui"' \
              -search_attr_key 'CustomStringField | CustomIntField | CustomKeywordField | CustomBoolField | CustomDatetimeField' \
              -search_attr_value "vancexu ${i} | ${COSTINT} | keyword1 | true | 2019-06-07T16:16:36-08:00"
done