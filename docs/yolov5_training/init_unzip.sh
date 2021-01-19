#!/bin/bash
# rm -r ~/yolov5_training/data_original/*
mkdir -p ~/yolov5_training/tmp
mkdir -p ~/yolov5_training/data_original
cd ~/yolov5_training/zip/$1
# get one zip file name
FNAME=$(ls | grep .zip | head -n1)
unzip -j "$FNAME" "obj.names" -d ~/yolov5_training/tmp/

# unzip images
n=0; for i in *.zip; do unzip -j "$i" "obj_train_data/*" -d ~/yolov5_training/data_original/"${n}"/; n=$((n+1)); done
